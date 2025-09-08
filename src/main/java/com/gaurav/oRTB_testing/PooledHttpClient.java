package com.gaurav.oRTB_testing;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

public class PooledHttpClient implements AutoCloseable {
    private final EventLoopGroup group;
    private final Class<? extends Channel> clientChannelClass;
    private final Map<String, ChannelPool> pools = new ConcurrentHashMap<>();

    private final int respMaxBytes;
    private final int aggMaxBytes;
    private final int connectMs;
    private final int maxPerHost;
    private final int maxPending;
    private final long acquireTimeoutMs;

    private static final AttributeKey<Pending> PENDING_KEY = AttributeKey.valueOf("pendingReq");

    private static final class Pending {
        final CompletableFuture<UpstreamResult> cf;
        volatile ScheduledFuture<?> killer; // scheduled timeout task
        Pending(CompletableFuture<UpstreamResult> cf) { this.cf = cf; }
    }

    @ChannelHandler.Sharable
    private final class RespHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
            Pending p = ctx.channel().attr(PENDING_KEY).getAndSet(null);
            if (p == null) {
                ctx.fireChannelRead(msg.retain());
                return;
            }
            try {
                if (p.killer != null) p.killer.cancel(false);
                String body = "";
                if (respMaxBytes > 0 && msg.content().isReadable()) {
                    int len = Math.min(respMaxBytes, msg.content().readableBytes());
                    body = msg.content().readCharSequence(len, StandardCharsets.UTF_8).toString();
                }
                p.cf.complete(new UpstreamResult(msg.status().code(), body));
            } finally {
                // release/close handled by whenComplete()
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            Pending p = ctx.channel().attr(PENDING_KEY).getAndSet(null);
            if (p != null && !p.cf.isDone()) {
                if (p.killer != null) p.killer.cancel(false);
                p.cf.completeExceptionally(cause);
            }
            ctx.close();
        }
    }

    private final RespHandler sharedHandler = new RespHandler();

    public PooledHttpClient(
            EventLoopGroup group, Class<? extends Channel> clientChannelClass,
            int respMaxBytes, int aggMaxBytes, int connectMs,
            int maxPerHost, int maxPending, long acquireTimeoutMs) {

        this.group = group;
        this.clientChannelClass = clientChannelClass;
        this.respMaxBytes = respMaxBytes;
        this.aggMaxBytes = aggMaxBytes;
        this.connectMs = connectMs;
        this.maxPerHost = maxPerHost;
        this.maxPending = maxPending;
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    // Back-compat ctor
    public PooledHttpClient(EventLoopGroup group, Class<? extends Channel> clientChannelClass) {
        this(group, clientChannelClass,
                Integer.getInteger("resp.max.bytes", 256),
                Integer.getInteger("netty.client.aggMaxBytes", 4096),
                Integer.getInteger("netty.connect.ms", 60),
                Integer.getInteger("netty.pool.maxPerHost", 200),
                Integer.getInteger("netty.pool.maxPending", 1000),
                Long.getLong("netty.pool.acquireTimeoutMs", 50L));
    }

    public CompletableFuture<UpstreamResult> get(String host, int port, String path,
                                                 Duration perRequestTimeout, long deadlineNanos) {
        return send(host, port, path, null, perRequestTimeout, deadlineNanos, false);
    }

    public CompletableFuture<UpstreamResult> postJson(String host, int port, String path, byte[] json,
                                                      Duration perRequestTimeout, long deadlineNanos) {
        return send(host, port, path, json, perRequestTimeout, deadlineNanos, true);
    }

    private CompletableFuture<UpstreamResult> send(
            String host, int port, String path, byte[] body,
            Duration perRequestTimeout, long deadlineNanos, boolean isPost) {

        long now = System.nanoTime();
        long leftNsBeforeAcquire = deadlineNanos - now;
        if (leftNsBeforeAcquire <= 0) {
            CompletableFuture<UpstreamResult> tooLate = new CompletableFuture<>();
            tooLate.completeExceptionally(new TimeoutException("budget exhausted before acquire"));
            return tooLate;
        }

        String key = host + ":" + port;
        ChannelPool pool = pools.computeIfAbsent(key, k -> buildPool(host, port));
        CompletableFuture<UpstreamResult> cf = new CompletableFuture<>();

        pool.acquire().addListener((io.netty.util.concurrent.Future<Channel> acq) -> {
            if (!acq.isSuccess()) { cf.completeExceptionally(acq.cause()); return; }
            final Channel ch = acq.getNow();
            final Pending pending = new Pending(cf);
            ch.attr(PENDING_KEY).set(pending);

            long leftNs = Math.max(0, deadlineNanos - System.nanoTime());
            long rawBudgetNs = Math.min(leftNs, perRequestTimeout.toNanos());
            final long budgetNs = (rawBudgetNs > 0) ? rawBudgetNs : 1_000_000L; // 1ms min

            // schedule timeout on the event loop (no per-request virtual thread)
            pending.killer = ch.eventLoop().schedule(() -> {
                if (cf.isDone()) return;
                ch.attr(PENDING_KEY).set(null);
                cf.completeExceptionally(new TimeoutException("upstream deadline"));
                ch.close();
            }, budgetNs, TimeUnit.NANOSECONDS);

            final FullHttpRequest req = isPost
                    ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path,
                    Unpooled.wrappedBuffer(body == null ? new byte[0] : body))
                    : new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);

            if (isPost) {
                req.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                req.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body == null ? 0 : body.length);
            }
            req.headers().set(HttpHeaderNames.HOST, host);
            req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            // avoid gzip to reduce CPU/heap
            req.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.IDENTITY);

            ch.writeAndFlush(req).addListener(w -> {
                if (!w.isSuccess()) {
                    ch.attr(PENDING_KEY).set(null);
                    if (pending.killer != null) pending.killer.cancel(false);
                    cf.completeExceptionally(w.cause());
                }
            });

            cf.whenComplete((v, e) -> {
                if (pending.killer != null) pending.killer.cancel(false);
                if (ch.isActive()) pool.release(ch);
                else { try { ch.close(); } catch (Exception ignore) {} }
            });
        });

        return cf;
    }

    private ChannelPool buildPool(String host, int port) {
        Bootstrap bs = new Bootstrap()
                .group(group)
                .channel(clientChannelClass)
                .remoteAddress(host, port)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectMs);

        return new FixedChannelPool(
                bs,
                new ChannelPoolHandler() {
                    @Override public void channelReleased(Channel ch) { }
                    @Override public void channelAcquired(Channel ch) { }
                    @Override public void channelCreated(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpContentDecompressor());
                        p.addLast(new HttpObjectAggregator(aggMaxBytes)); // keep small
                        p.addLast(sharedHandler);
                    }
                },
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                acquireTimeoutMs,
                maxPerHost,
                maxPending
        );
    }

    @Override public void close() { pools.clear(); }
}