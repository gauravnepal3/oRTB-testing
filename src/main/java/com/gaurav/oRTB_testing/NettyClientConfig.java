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
        return Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup();
    }

    @Bean(destroyMethod = "close")
    public PooledHttpClient pooledHttpClient(EventLoopGroup clientGroup) {
        Class<? extends io.netty.channel.Channel> ch = Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

        return new PooledHttpClient(
                clientGroup, ch,
                Integer.getInteger("resp.max.bytes", 256),
                Integer.getInteger("netty.client.aggMaxBytes", 4096),
                Integer.getInteger("netty.connect.ms", 60),
                Integer.getInteger("netty.pool.maxPerHost", 200),
                Integer.getInteger("netty.pool.maxPending", 800),
                Long.getLong("netty.pool.acquireTimeoutMs", 50)
        );
    }
}