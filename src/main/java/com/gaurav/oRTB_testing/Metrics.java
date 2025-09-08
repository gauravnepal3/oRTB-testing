package com.gaurav.oRTB_testing;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

class Metrics {
    private final LongAdder count = new LongAdder();
    private final int reservoirSize = 100_000;
    private final long[] reservoir = new long[reservoirSize];
    private final AtomicInteger idx = new AtomicInteger();
    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "metrics"); t.setDaemon(true); return t; });

    void record(long durMs) {
        count.increment();
        int i = idx.getAndIncrement(); reservoir[i % reservoirSize] = durMs;
    }

    void startConsoleReporter() {
        final LongAdder last = new LongAdder();
        ses.scheduleAtFixedRate(() -> {
            long total = count.sum();
            long prev = last.sum();
            long diff = total - prev;
            last.add(diff);

            int n = Math.min((int)Math.min(total, reservoirSize), reservoirSize);
            long[] snap = new long[n];
            for (int i = 0; i < n; i++) snap[i] = reservoir[i];
            Arrays.sort(snap);
            long p50 = snap.length == 0 ? 0 : snap[(int)(0.50 * (snap.length - 1))];
            long p95 = snap.length == 0 ? 0 : snap[(int)(0.95 * (snap.length - 1))];
            long p99 = snap.length == 0 ? 0 : snap[(int)(0.99 * (snap.length - 1))];

            System.out.printf(Locale.ROOT, "QPS=%d | p50=%dms p95=%dms p99=%dms | count=%d ",
                    diff, p50, p95, p99, total);
        }, 1, 1, TimeUnit.SECONDS);
    }
}