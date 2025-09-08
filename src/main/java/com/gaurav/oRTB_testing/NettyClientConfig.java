package com.gaurav.oRTB_testing;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NettyClientConfig {

    @Bean(destroyMethod = "shutdownGracefully")
    public EventLoopGroup clientGroup() {
        if (Epoll.isAvailable()) return new EpollEventLoopGroup();
        return new NioEventLoopGroup();
    }

    @Bean(destroyMethod = "close")
    public PooledHttpClient pooledHttpClient(
            EventLoopGroup clientGroup,
            @Value("${resp.max.bytes:65536}") int respMaxBytes,
            @Value("${netty.client.aggMaxBytes:65536}") int aggMaxBytes,
            @Value("${netty.connect.ms:80}") int connectMs,
            @Value("${netty.pool.maxPerHost:4000}") int maxPerHost,
            @Value("${netty.pool.maxPending:20000}") int maxPending,
            @Value("${netty.pool.acquireTimeoutMs:2}") long acquireTimeoutMs
    ) {
        Class<? extends io.netty.channel.Channel> ch =
                Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

        return new PooledHttpClient(
                clientGroup, ch,
                respMaxBytes, aggMaxBytes, connectMs,
                maxPerHost, maxPending, acquireTimeoutMs
        );
    }
}