package com.gaurav.oRTB_testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/openrtb2")
public class OpenRtbController {
    private static final ObjectMapper M = new ObjectMapper();

    private final PooledHttpClient http;
    private final List<Target> targets;
    private final int defaultFanout;
    private final int totalTimeoutMs;
    private final NonBlockingKafkaPublisher publisher;
    private final ExecutorService vexec;
    private final MeterRegistry meter;

    @Value("${auction.immediateAck:true}")
    private boolean immediateAck;

    // ---- Backpressure (global across requests) ----
    private static final int MAX_INFLIGHT =
            Integer.getInteger("fanout.maxInflight",
                    Integer.parseInt(System.getenv().getOrDefault("FANOUT_MAX_INFLIGHT", "1800")));
    private static final Semaphore FANOUT_SEM = new Semaphore(Math.max(100, MAX_INFLIGHT));

    // ---- Metrics names ----
    private static final String METRIC_OUT_REQS     = "rtb.outgoing.requests";
    private static final String METRIC_OUT_LATENCY  = "rtb.outgoing.latency";
    private static final String METRIC_OUT_INFLIGHT = "rtb.outgoing.inflight";
    private static final String METRIC_IN_REQS      = "rtb.incoming.auction.requests";

    public OpenRtbController(
            @Qualifier("vthreadExecutor") ExecutorService vexec,
            PooledHttpClient http,
            NonBlockingKafkaPublisher publisher,
            MeterRegistry meter,
            @Value("${external.targets}") String targetsCsv,
            @Value("${fanout.count:5}") int defaultFanout,
            @Value("${fanout.totalTimeoutMs:300}") int totalTimeoutMs) {

        this.vexec = vexec;
        this.http = http;
        this.defaultFanout = defaultFanout;
        this.totalTimeoutMs = totalTimeoutMs;
        this.publisher = publisher;
        this.meter = meter;

        // Parse and validate targets
        List<Target> t = new ArrayList<>();
        for (String s : targetsCsv.split(",")) {
            s = s.trim();
            if (s.isEmpty()) continue;
            URI u = URI.create(s);
            String host = u.getHost();
            if (host == null || host.isBlank())
                throw new IllegalArgumentException("Bad target URL: " + s);
            int port = (u.getPort() == -1) ? ("https".equalsIgnoreCase(u.getScheme()) ? 443 : 80) : u.getPort();
            String path = (u.getRawPath() == null || u.getRawPath().isEmpty()) ? "/" : u.getRawPath();
            if (u.getRawQuery() != null && !u.getRawQuery().isEmpty()) path = path + "?" + u.getRawQuery();
            t.add(new Target(host, port, path, u.toString()));
        }
        if (t.size() < defaultFanout) {
            throw new IllegalArgumentException("Need at least " + defaultFanout + " external.targets");
        }
        this.targets = Collections.unmodifiableList(t);

        // Register an inflight gauge (computed from the semaphore)
        // Shows current number of busy permits (i.e., active upstream calls)
        meter.gauge(METRIC_OUT_INFLIGHT, Tags.empty(), this, OpenRtbController::currentInflight);
    }

    // Current number of in-flight upstream requests
    private double currentInflight() {
        return (double) (Math.max(0, MAX_INFLIGHT - FANOUT_SEM.availablePermits()));
    }

    // Shared tiny scheduler for deadlines
    private static final ScheduledExecutorService DEADLINE_SCHED =
            Executors.newScheduledThreadPool(
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 4),
                    r -> { Thread td = new Thread(r, "deadline-sched"); td.setDaemon(true); return td; });

    @PostMapping(path = "/auction", consumes = "application/json")
    public ResponseEntity<Void> auction(@RequestBody byte[] body) {
        // Record incoming request (optional; http.server.requests is also available)
        meter.counter(METRIC_IN_REQS).increment();
        meter.counter("rtb_incoming_requests_total", "route", "auction").increment();

        final int tmaxMs = quickTmax(body, totalTimeoutMs);
        final String openrtbId = quickId(body);
        final UUID reqUuid = UUID.randomUUID();

        // Log request off the hot path (Kafka producer is a background client)
        publisher.publishRequest(reqUuid, openrtbId, tmaxMs, defaultFanout);

        if (immediateAck) {
            // Fire-and-forget fanout
            vexec.execute(() -> fanoutAndLog(reqUuid, body, tmaxMs));
            return ResponseEntity.noContent().header("X-Ack", "immediate").build();
        } else {
            // Synchronous: wait up to tmax for fanout to complete
            final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
            List<CompletableFuture<UpstreamResult>> calls = new ArrayList<>(defaultFanout);
            for (int i = 0; i < defaultFanout; i++) {
                Target tgt = targets.get(i % targets.size());
                calls.add(callOne(tgt, reqUuid, body, deadlineNanos, tmaxMs));
            }
            try { allDoneOrDeadline(calls, tmaxMs, TimeUnit.MILLISECONDS).join(); } catch (Exception ignore) {}
            return ResponseEntity.noContent().header("X-Ack", "waited").build();
        }
    }

    private void fanoutAndLog(UUID reqUuid, byte[] body, int tmaxMs) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
        for (int i = 0; i < defaultFanout; i++) {
            Target tgt = targets.get(i % targets.size());
            callOne(tgt, reqUuid, body, deadlineNanos, tmaxMs);
        }
    }

    private CompletableFuture<UpstreamResult> callOne(Target tgt, UUID reqUuid, byte[] body, long deadlineNanos, int tmaxMs) {
        // Backpressure: if no permit, drop instantly and log/metric as rejected
        if (!FANOUT_SEM.tryAcquire()) {
            publisher.publishResponse(reqUuid, tgt.url, 0, 0, true, "");
            recordOutgoingMetrics(tgt.url, true, 0, 0);
            return CompletableFuture.completedFuture(null);
        }

        final long start = System.nanoTime();
        final int remainingMs = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

        return http.postJson(tgt.host, tgt.port, tgt.path, body, Duration.ofMillis(remainingMs), deadlineNanos)
                .handle((res, err) -> {
                    final boolean dropped = (err != null);
                    final int status = dropped ? 0 : res.status();
                    final String respBody = dropped ? "" : res.body();  // body captured (truncated by resp.max.bytes if configured)
                    final int durMs = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                    publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, respBody);
                    recordOutgoingMetrics(tgt.url, dropped, status, durMs);
                    return res;
                })
                .whenComplete((__, ___) -> FANOUT_SEM.release());
    }



    private void recordOutgoingMetrics(String targetUrl, boolean dropped, int status, int durMs) {
        String outcome = dropped ? "dropped" : (status >= 200 && status < 400 ? "ok" : "error");
        Tags tags = Tags.of("target", targetUrl, "outcome", outcome);
        meter.counter(METRIC_OUT_REQS, tags).increment();
        meter.counter("rtb_outgoing_requests_total", "target", targetUrl, "outcome", outcome).increment();
        meter.timer("rtb_outgoing_duration_seconds", "target", targetUrl, "outcome", outcome)
                .record(durMs, TimeUnit.MILLISECONDS);
        if (!dropped) {
            meter.timer(METRIC_OUT_LATENCY, tags).record(durMs, TimeUnit.MILLISECONDS);
        }
    }

    @GetMapping("/ping")
    public ResponseEntity<Void> ping(){ return ResponseEntity.noContent().build(); }

    // ===== helpers =====
    private static CompletableFuture<Void> allDoneOrDeadline(
            List<CompletableFuture<UpstreamResult>> cfs, long time, TimeUnit unit) {

        CompletableFuture<?>[] arr = cfs.stream()
                .map(f -> f.exceptionally(ex -> null))
                .toArray(CompletableFuture[]::new);

        CompletableFuture<Void> all = CompletableFuture.allOf(arr);
        CompletableFuture<Void> timer = new CompletableFuture<>();
        DEADLINE_SCHED.schedule(() -> timer.complete(null), time, unit);
        return all.applyToEither(timer, v -> null);
    }

    private static int quickTmax(byte[] body, int def) {
        // naive fast scan: ..."tmax":123...
        int n = body.length;
        for (int i = 0; i < n - 6; i++) {
            if (body[i]=='t' && body[i+1]=='m' && body[i+2]=='a' && body[i+3]=='x') {
                int j = i + 4;
                while (j < n && body[j] != ':') j++;
                j++;
                int val = 0, digits = 0;
                while (j < n) {
                    byte b = body[j++];
                    if (b >= '0' && b <= '9') { val = val*10 + (b - '0'); digits++; if (digits > 6) break; }
                    else if (digits > 0) break;
                }
                return digits > 0 ? val : def;
            }
        }
        return def;
    }

    private static String quickId(byte[] body) {
        try { JsonNode n = M.readTree(body); return n.has("id") ? n.get("id").asText("") : ""; }
        catch (Exception e) { return ""; }
    }

    private record Target(String host, int port, String path, String url) {}
}