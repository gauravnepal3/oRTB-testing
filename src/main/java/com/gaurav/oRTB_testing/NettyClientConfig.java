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

    // NettyClientConfig.java
    @Bean(destroyMethod = "close")
    public PooledHttpClient pooledHttpClient(EventLoopGroup clientGroup) {
        Class<? extends io.netty.channel.Channel> ch =
                Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

        return new PooledHttpClient(
                clientGroup, ch,
                Integer.getInteger("resp.max.bytes", 65536),            // capture body cap
                Integer.getInteger("netty.client.aggMaxBytes", 262144), // aggregator
                Integer.getInteger("netty.connect.ms", 100),            // connect timeout
                Integer.getInteger("netty.pool.maxPerHost", 2000),      // pool size / host
                Integer.getInteger("netty.pool.maxPending", 50000),     // pending acquires
                Long.getLong("netty.pool.acquireTimeoutMs", 150)        // <-- key change
        );
    }
}