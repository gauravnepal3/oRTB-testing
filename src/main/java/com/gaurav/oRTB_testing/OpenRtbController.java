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
import java.util.concurrent.atomic.LongAdder;

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

    // ---- Metrics names (counters/timers you already use) ----
    private static final String METRIC_OUT_REQS     = "rtb.outgoing.requests";
    private static final String METRIC_OUT_LATENCY  = "rtb.outgoing.latency";
    private static final String METRIC_OUT_INFLIGHT = "rtb.outgoing.inflight";
    private static final String METRIC_IN_REQS      = "rtb.incoming.auction.requests";

    // ---- Live RPS calculators (instant-ish gauges) ----
    private final RollingRps inbound1s  = new RollingRps(1);
    private final RollingRps inbound5s  = new RollingRps(5);
    private final RollingRps outgoing1s = new RollingRps(1);
    private final RollingRps outgoing5s = new RollingRps(5);

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

        // Inflight gauge
        meter.gauge(METRIC_OUT_INFLIGHT, Tags.empty(), this, OpenRtbController::currentInflight);

        // Live RPS gauges (Micrometer will export with dots->underscores in Prometheus)
        meter.gauge("rtb.live.inbound.rps.1s",  Tags.empty(), inbound1s,  RollingRps::perSecond);
        meter.gauge("rtb.live.inbound.rps.5s",  Tags.empty(), inbound5s,  RollingRps::perSecond);
        meter.gauge("rtb.live.outgoing.rps.1s", Tags.empty(), outgoing1s, RollingRps::perSecond);
        meter.gauge("rtb.live.outgoing.rps.5s", Tags.empty(), outgoing5s, RollingRps::perSecond);
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
        // Increment counters AND live RPS ring
        meter.counter(METRIC_IN_REQS, Tags.of("route", "auction")).increment();
        inbound1s.mark();
        inbound5s.mark();

        final int tmaxMs = quickTmax(body, totalTimeoutMs);
        final String openrtbId = quickId(body);
        final UUID reqUuid = UUID.randomUUID();

        // Log request off the hot path (Kafka producer is a background client)
        publisher.publishRequest(reqUuid, openrtbId, tmaxMs, defaultFanout);

        if (immediateAck) {
            vexec.execute(() -> fanoutAndLog(reqUuid, body, tmaxMs));
            return ResponseEntity.noContent().header("X-Ack", "immediate").build();
        } else {
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
            // mark live outgoing counter too (a call attempt happened)
            outgoing1s.mark();
            outgoing5s.mark();
            return CompletableFuture.completedFuture(null);
        }

        final long start = System.nanoTime();
        final int remainingMs = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

        return http.postJson(tgt.host, tgt.port, tgt.path, body, Duration.ofMillis(remainingMs), deadlineNanos)
                .handle((res, err) -> {
                    final boolean dropped = (err != null);
                    final int status = dropped ? 0 : res.status();
                    final String respBody = dropped ? "" : res.body();
                    final int durMs = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                    publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, respBody);
                    recordOutgoingMetrics(tgt.url, dropped, status, durMs);

                    // live outgoing RPS mark (count every upstream attempt)
                    outgoing1s.mark();
                    outgoing5s.mark();
                    return res;
                })
                .whenComplete((__, ___) -> FANOUT_SEM.release());
    }

    private void recordOutgoingMetrics(String targetUrl, boolean dropped, int status, int durMs) {
        String outcome = dropped ? "dropped" : (status >= 200 && status < 400 ? "ok" : "error");
        Tags tags = Tags.of("target", targetUrl, "outcome", outcome);
        meter.counter(METRIC_OUT_REQS, tags).increment();
        // one timer (dotted name) for duration distribution
        meter.timer("rtb.outgoing.duration", "target", targetUrl, "outcome", outcome)
                .record(durMs, TimeUnit.MILLISECONDS);
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

    /**
     * Rolling per-second counter over a small sliding window (N seconds).
     * - mark(): record one event at "now"
     * - perSecond(): return average RPS over the last N seconds (for N=1, it's the last second bucket)
     *
     * Lock-free fast path using epoch-second bucketing; advancing is synchronized only when we cross seconds.
     */
    static final class RollingRps {
        private final int windowSec;
        private final LongAdder[] buckets;
        private volatile long headSec; // epoch second of current head bucket
        private volatile int headIdx;  // index of current head bucket
        private final Object advanceLock = new Object();

        RollingRps(int windowSec) {
            if (windowSec < 1) throw new IllegalArgumentException("windowSec must be >= 1");
            this.windowSec = windowSec;
            this.buckets = new LongAdder[windowSec];
            for (int i = 0; i < windowSec; i++) buckets[i] = new LongAdder();
            long now = currentSec();
            this.headSec = now;
            this.headIdx = 0;
        }

        void mark() {
            long now = currentSec();
            advance(now);
            buckets[headIdx].increment();
        }

        double perSecond() {
            long now = currentSec();
            advance(now); // ensure buckets are up to date
            long sum = 0;
            for (int i = 0; i < windowSec; i++) sum += buckets[i].sum();
            // average per second across the window
            return (double) sum / (double) windowSec;
        }

        private static long currentSec() {
            return System.currentTimeMillis() / 1000L;
            // Alternatively for lower overhead: TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()) with an epoch offset
        }

        private void advance(long nowSec) {
            long delta = nowSec - headSec;
            if (delta <= 0) return; // same second
            // We crossed one or more seconds; rotate buckets
            synchronized (advanceLock) {
                long d = nowSec - headSec;
                if (d <= 0) return;
                int steps = (int) Math.min(d, windowSec); // don't rotate more than window size
                for (int i = 0; i < steps; i++) {
                    headIdx = (headIdx + 1) % windowSec;
                    buckets[headIdx] = new LongAdder(); // reset bucket we’re rotating into
                }
                headSec = nowSec;
            }
        }
    }
}