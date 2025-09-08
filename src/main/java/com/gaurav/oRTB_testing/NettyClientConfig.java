package com.gaurav.oRTB_testing;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
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
    public PooledHttpClient pooledHttpClient(EventLoopGroup clientGroup) {
        Class<? extends io.netty.channel.Channel> ch =
                Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

        return new PooledHttpClient(
                clientGroup, ch,
                // capture body cap (system property or default 0)
                Integer.getInteger("resp.max.bytes", 0),
                // aggregate only small bodies (system property or default 8192)
                Integer.getInteger("netty.client.aggMaxBytes", 8192),
                Integer.getInteger("netty.connect.ms", 100),
                Integer.getInteger("netty.pool.maxPerHost", 2000),
                Integer.getInteger("netty.pool.maxPending", 50000),
                Long.getLong("netty.pool.acquireTimeoutMs", 150L)
        );
    }
}