package com.gaurav.oRTB_testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final PooledHttpClient http;          // Netty client
    private final List<Target> targets;           // host/port/path
    private final int defaultFanout;
    private final int totalTimeoutMs;
    private final NonBlockingKafkaPublisher publisher;
    private final ExecutorService vexec;

    // admission control caps (system props overridable)
    private final Semaphore inflightAuctions =
            new Semaphore(Integer.getInteger("auction.maxInflight", 2000));
    private final Semaphore inflightUpstreams =
            new Semaphore(Integer.getInteger("upstream.maxInflight", 8000));

    @Value("${auction.immediateAck:true}")
    private boolean immediateAck;

    public OpenRtbController(
            @Qualifier("vthreadExecutor") ExecutorService vexec,
            PooledHttpClient http,
            NonBlockingKafkaPublisher publisher,
            @Value("${external.targets}") String targetsCsv,
            @Value("${fanout.count:5}") int defaultFanout,
            @Value("${fanout.totalTimeoutMs:300}") int totalTimeoutMs
    ) {
        this.vexec = vexec;
        this.http = http;
        this.publisher = publisher;
        this.defaultFanout = defaultFanout;
        this.totalTimeoutMs = totalTimeoutMs;

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
    }

    // shared scheduler for "wait for all OR deadline"
    private static final ScheduledExecutorService DEADLINE_SCHED =
            Executors.newScheduledThreadPool(
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 4),
                    r -> { Thread th = new Thread(r, "deadline-sched"); th.setDaemon(true); return th; }
            );

    @PostMapping(path = "/auction", consumes = "application/json")
    public ResponseEntity<Void> auction(@RequestBody byte[] body) {
        if (!inflightAuctions.tryAcquire()) {
            // overload protection
            return ResponseEntity.status(503).header("X-Dropped", "auction-cap").build();
        }
        try {
            final int tmaxMs = quickTmax(body, totalTimeoutMs);
            final String openrtbId = quickId(body);
            final UUID reqUuid = UUID.randomUUID();

            // Log request off the hot path
            publisher.publishRequest(reqUuid, openrtbId, tmaxMs, defaultFanout);

            if (immediateAck) {
                // Fire-and-forget: do fanout on a virtual thread and return 204 now
                vexec.execute(() -> fanoutAndLog(reqUuid, body, tmaxMs));
                return ResponseEntity.noContent()
                        .header("X-Ack", "immediate")
                        .build();
            } else {
                // Synchronous: issue fanout here and wait up to tmax, then 204
                final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
                List<CompletableFuture<UpstreamResult>> calls = new ArrayList<>(defaultFanout);

                for (int i = 0; i < defaultFanout; i++) {
                    Target tgt = targets.get(i % targets.size());

                    if (!inflightUpstreams.tryAcquire()) {
                        // back-pressure: record a drop and skip
                        publisher.publishResponse(reqUuid, tgt.url, 0, 0, true, "");
                        continue;
                    }

                    final long start = System.nanoTime();
                    int remainingMs = (int) Math.max(1,
                            TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

                    CompletableFuture<UpstreamResult> cf = http
                            .postJson(tgt.host, tgt.port, tgt.path, body,
                                    Duration.ofMillis(remainingMs), deadlineNanos)
                            .handle((res, err) -> {
                                try {
                                    final boolean dropped = (err != null);
                                    final int status = dropped ? 0 : res.status();
                                    final String respBody = dropped ? "" : res.body();
                                    final int durMs = (int) TimeUnit.NANOSECONDS
                                            .toMillis(System.nanoTime() - start);
                                    publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, respBody);
                                } finally {
                                    inflightUpstreams.release();
                                }
                                return null;
                            });

                    calls.add(cf);
                }

                try {
                    allDoneOrDeadline(calls, tmaxMs, TimeUnit.MILLISECONDS).join();
                } catch (Exception ignored) { /* deadline or cancellation is fine */ }

                return ResponseEntity.noContent()
                        .header("X-Ack", "waited")
                        .build();
            }
        } finally {
            inflightAuctions.release();
        }
    }

    private void fanoutAndLog(UUID reqUuid, byte[] body, int tmaxMs) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
        for (int i = 0; i < defaultFanout; i++) {
            Target tgt = targets.get(i % targets.size());

            if (!inflightUpstreams.tryAcquire()) {
                publisher.publishResponse(reqUuid, tgt.url, 0, 0, true, "");
                continue;
            }

            final long start = System.nanoTime();
            int remainingMs = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

            http.postJson(tgt.host, tgt.port, tgt.path, body, Duration.ofMillis(remainingMs), deadlineNanos)
                    .handle((res, err) -> {
                        try {
                            final boolean dropped = (err != null);
                            final int status = dropped ? 0 : res.status();
                            final String respBody = dropped ? "" : res.body();
                            final int durMs = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                            // order: (uuid, target, status, durationMs, dropped, body)
                            publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, respBody);
                        } finally {
                            inflightUpstreams.release();
                        }
                        return null;
                    });
        }
    }

    @GetMapping("/ping")
    public ResponseEntity<Void> ping() {
        return ResponseEntity.noContent().build();
    }

    // shared helper (uses the shared DEADLINE_SCHED)
    private static CompletableFuture<Void> allDoneOrDeadline(
            List<CompletableFuture<UpstreamResult>> cfs, long time, TimeUnit unit) {

        CompletableFuture<?>[] arr = cfs.stream()
                .map(f -> f.exceptionally(ex -> null))
                .toArray(CompletableFuture[]::new);

        CompletableFuture<Void> all = CompletableFuture.allOf(arr);
        CompletableFuture<Void> timer = new CompletableFuture<>();
        DEADLINE_SCHED.schedule(() -> timer.complete(null), time, unit);

        // Completes when either 'all' or the timer completes
        return all.applyToEither(timer, v -> null);
    }

    private static int quickTmax(byte[] body, int def) {
        // naive fast scan: ..."tmax":123...
        int n = body.length;
        for (int i = 0; i < n - 6; i++) {
            if (body[i] == 't' && body[i+1]=='m' && body[i+2]=='a' && body[i+3]=='x') {
                // seek ':' then digits
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
        // trivial fallback: just parse with Jackson for id (rarely needed in hot path)
        try {
            JsonNode n = M.readTree(body);
            return n.has("id") ? n.get("id").asText("") : "";
        } catch (Exception e) {
            return "";
        }
    }

    private record Target(String host, int port, String path, String url) {}
}