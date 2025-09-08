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

    @Value("${auction.immediateAck:true}")
    private boolean immediateAck;

    public OpenRtbController(
            @Qualifier("vthreadExecutor") ExecutorService vexec,
            PooledHttpClient http,
            NonBlockingKafkaPublisher publisher,
            @Value("${external.targets}") String targetsCsv,
            @Value("${fanout.count:5}") int defaultFanout,
            @Value("${fanout.totalTimeoutMs:300}") int totalTimeoutMs) {

        this.vexec = vexec;
        this.http = http;
        this.defaultFanout = defaultFanout;
        this.totalTimeoutMs = totalTimeoutMs;
        this.publisher = publisher;

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

    private static final ScheduledExecutorService DEADLINE_SCHED =
            Executors.newScheduledThreadPool(
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 4),
                    r -> { Thread t = new Thread(r, "deadline-sched"); t.setDaemon(true); return t; }
            );

    @PostMapping(path = "/auction", consumes = "application/json")
    public ResponseEntity<Void> auction(@RequestBody byte[] body) {
        final int tmaxMs = quickTmax(body, totalTimeoutMs);
        // HOT PATH: skip parsing OpenRTB id to maximize QPS
        final String openrtbId = ""; // (set to "" for throughput tests)
        final UUID reqUuid = UUID.randomUUID();

        publisher.publishRequest(reqUuid, openrtbId, tmaxMs, defaultFanout);

        if (immediateAck) {
            vexec.execute(() -> fanoutAndLog(reqUuid, body, tmaxMs));
            return ResponseEntity.noContent()
                    .header("X-Ack", "immediate")
                    .build();
        } else {
            final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
            List<CompletableFuture<UpstreamResult>> calls = new ArrayList<>(defaultFanout);

            for (int i = 0; i < defaultFanout; i++) {
                Target tgt = targets.get(i % targets.size());
                final long start = System.nanoTime();
                int remainingMs = (int) Math.max(1,
                        TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

                CompletableFuture<UpstreamResult> cf = http
                        .postJson(tgt.host, tgt.port, tgt.path, body,
                                Duration.ofMillis(remainingMs), deadlineNanos)
                        .handle((res, err) -> {
                            final boolean dropped = (err != null);
                            final int status = dropped ? 0 : res.status();
                            final int durMs = (int) TimeUnit.NANOSECONDS
                                    .toMillis(System.nanoTime() - start);

                            final String respBody = dropped ? "" : res.body();
                            // keep only id + took_ms to ClickHouse
                            final String compact = dropped ? "" : extractIdAndTook(respBody);
                            publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, compact);
                            return res;
                        });

                calls.add(cf);
            }

            try {
                allDoneOrDeadline(calls, tmaxMs, TimeUnit.MILLISECONDS).join();
            } catch (Exception ignored) { }

            return ResponseEntity.noContent()
                    .header("X-Ack", "waited")
                    .build();
        }
    }
    private static String extractIdAndTook(String s) {
        if (s == null || s.isEmpty()) return "";
        String id = fastString(s, "\"id\"");
        int took = fastInt(s, "\"took_ms\"");
        if (id == null && took < 0) return "";
        StringBuilder sb = new StringBuilder(48).append('{');
        boolean first = true;
        if (id != null) { sb.append("\"id\":\"").append(id).append('"'); first = false; }
        if (took >= 0) { if (!first) sb.append(','); sb.append("\"took_ms\":").append(took); }
        sb.append('}');
        return sb.toString();
    }
    private static String fastString(String s, String key) {
        int i = s.indexOf(key); if (i < 0) return null;
        i = s.indexOf(':', i); if (i < 0) return null; i++;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        if (i >= s.length() || s.charAt(i) != '"') return null;
        i++;
        int j = i;
        while (j < s.length() && s.charAt(j) != '"') j++;
        return (j <= s.length()) ? s.substring(i, j) : null;
    }
    private static int fastInt(String s, String key) {
        int i = s.indexOf(key); if (i < 0) return -1;
        i = s.indexOf(':', i); if (i < 0) return -1; i++;
        while (i < s.length() && !Character.isDigit(s.charAt(i))) i++;
        int start = i, val = 0;
        while (i < s.length() && Character.isDigit(s.charAt(i))) { val = val * 10 + (s.charAt(i) - '0'); i++; }
        return (i == start) ? -1 : val;
    }

    private void fanoutAndLog(UUID reqUuid, byte[] body, int tmaxMs) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(tmaxMs);
        for (int i = 0; i < defaultFanout; i++) {
            Target tgt = targets.get(i % targets.size());
            final long start = System.nanoTime();
            int remainingMs = (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));

            http.postJson(tgt.host, tgt.port, tgt.path, body, Duration.ofMillis(remainingMs), deadlineNanos)
                    .handle((res, err) -> {
                        final boolean dropped = (err != null);
                        final int status = dropped ? 0 : res.status();
                        final String respBody = dropped ? "" : res.body();
                        final int durMs = (int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                        publisher.publishResponse(reqUuid, tgt.url, status, durMs, dropped, respBody);
                        return null;
                    });
        }
    }

    @GetMapping("/ping") public ResponseEntity<Void> ping(){ return ResponseEntity.noContent().build(); }

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
            if (body[i] == 't' && body[i+1]=='m' && body[i+2]=='a' && body[i+3]=='x') {
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

    // kept for future use; not used on the hot path in this perf build
    @SuppressWarnings("unused")
    private static String quickId(byte[] body) {
        try {
            JsonNode n = M.readTree(body);
            return n.has("id") ? n.get("id").asText("") : "";
        } catch (Exception e) {
            return "";
        }
    }

    private record Target(String host, int port, String path, String url) {}
}