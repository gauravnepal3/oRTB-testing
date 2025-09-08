// NonBlockingKafkaPublisher.java
package com.gaurav.oRTB_testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Component
public class NonBlockingKafkaPublisher {
    private final ObjectMapper M = new ObjectMapper();
    private final String bootstrap;
    private final String topicReq;
    private final String topicResp;
    private final String compression;
    private final int lingerMs;
    private final int batchBytes;
    private final int respMaxBytes;

    private Producer<String, String> producer;

    public NonBlockingKafkaPublisher(
            @Value("${kafka.bootstrap}") String bootstrap,
            @Value("${kafka.topic.requests:rtb_requests}") String topicReq,
            @Value("${kafka.topic.responses:rtb_responses}") String topicResp,
            @Value("${kafka.compression:lz4}") String compression,
            @Value("${kafka.lingerMs:1}") int lingerMs,
            @Value("${kafka.batchBytes:131072}") int batchBytes,
            @Value("${resp.max.bytes:65536}") int respMaxBytes
    ) {
        this.bootstrap = bootstrap;
        this.topicReq = topicReq;
        this.topicResp = topicResp;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.batchBytes = batchBytes;
        this.respMaxBytes = respMaxBytes;
    }

    @PostConstruct
    void init() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.ACKS_CONFIG, "0"); // fire-and-forget
        p.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, batchBytes);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        producer = new KafkaProducer<>(p);
    }

    public void publishRequest(UUID reqUuid, String openrtbId, int tmax, int fanout) {
        try {
            String json = M.writeValueAsString(Map.of(
                    "ts", System.currentTimeMillis(),
                    "req_uuid", reqUuid.toString(),
                    "openrtb_id", openrtbId == null ? "" : openrtbId,
                    "tmax", tmax,
                    "fanout", fanout
            ));
            producer.send(new ProducerRecord<>(topicReq, reqUuid.toString(), json));
        } catch (Exception ignore) {}
    }

    // inside publishResponse(...)
    public void publishResponse(UUID reqUuid, String target, int status, int durationMs, boolean dropped, String body) {
        try {
            final int limit = this.respMaxBytes; // Spring property
            // If limit <= 0 → don't trim at all (keeps body)
            final String trimmed = (body == null) ? "" :
                    (limit > 0 && body.length() > limit ? body.substring(0, limit) : body);

            String json = M.writeValueAsString(Map.of(
                    "ts", System.currentTimeMillis(),
                    "req_uuid", reqUuid.toString(),
                    "target", target,
                    "status_code", status,
                    "duration_ms", durationMs,
                    "dropped", dropped ? 1 : 0,
                    "resp_body", trimmed
            ));
            producer.send(new ProducerRecord<>(topicResp, reqUuid.toString(), json));
        } catch (Exception ignore) {}
    }

    @PreDestroy
    void close() {
        try { if (producer != null) { producer.flush(); producer.close(); } } catch (Exception ignore) {}
    }
}