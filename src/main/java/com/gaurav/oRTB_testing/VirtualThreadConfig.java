package com.gaurav.oRTB_testing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class VirtualThreadConfig {
    @Bean(name = "vthreadExecutor", destroyMethod = "close")
    @Primary
    public ExecutorService vthreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}