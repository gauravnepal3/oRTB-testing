package com.gaurav.oRTB_testing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.http.HttpClient;
import java.time.Duration;

/** JDK HttpClient (works great with virtual threads). */
@Configuration
public class HttpClientConfig {
    @Bean
    public HttpClient httpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(150)) // tune as you like
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }
}