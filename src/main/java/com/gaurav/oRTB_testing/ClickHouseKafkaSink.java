package com.gaurav.oRTB_testing;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class ClickHouseKafkaSink {
    private static final String INS_REQ = """
        INSERT INTO rtb_requests (ts, req_uuid, openrtb_id, tmax, fanout) VALUES (?, ?, ?, ?, ?)
        """;
    private static final String INS_RESP = """
        INSERT INTO rtb_responses (ts, req_uuid, target, status_code, duration_ms, dropped, resp_body) VALUES (?, ?, ?, ?, ?, ?, ?)
        """;

    private final ObjectMapper M = new ObjectMapper();
    private final ExecutorService vexec;
    private final String bootstrap;
    private final String topicReq;
    private final String topicResp;
    private final String groupId;
    private final DataSource ds;
    private final int maxBatch;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Consumer<String, String> consumer;

    public ClickHouseKafkaSink(
            @Qualifier("vthreadExecutor") ExecutorService vexec,
            @Value("${kafka.bootstrap}") String bootstrap,
            @Value("${kafka.topic.requests:rtb_requests}") String topicReq,
            @Value("${kafka.topic.responses:rtb_responses}") String topicResp,
            @Value("${kafka.group:rtb-ch-sink}") String groupId,
            @Value("${clickhouse.url}") String chUrl,
            @Value("${clickhouse.user:default}") String chUser,
            @Value("${clickhouse.password:}") String chPass,
            @Value("${clickhouse.sinkBatch:2000}") int maxBatch
    ) throws Exception {
        this.vexec = vexec;
        this.bootstrap = bootstrap;
        this.topicReq = topicReq;
        this.topicResp = topicResp;
        this.groupId = groupId;
        this.maxBatch = maxBatch;
        Properties props = new Properties();
        props.setProperty("user", chUser);
        props.setProperty("password", chPass);
        this.ds = new ClickHouseDataSource(chUrl, props);
        initTablesAndPing();
    }

    @PostConstruct
    void start() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // use 'latest' so we only consume what is produced after the app starts
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        consumer = new KafkaConsumer<>(p);
        consumer.subscribe(List.of(topicReq, topicResp));
        running.set(true);
        vexec.execute(this::loop);
        System.out.println("[CH-SINK] started; consuming " + topicReq + ", " + topicResp +
                " from " + bootstrap + " as group " + groupId);
    }

    private void loop() {
        try (Connection c = ds.getConnection()) {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) continue;

                List<JsonNode> reqs = new ArrayList<>(Math.min(maxBatch, records.count()));
                List<JsonNode> resps = new ArrayList<>(Math.min(maxBatch, records.count()));

                for (ConsumerRecord<String, String> r : records) {
                    try {
                        JsonNode n = M.readTree(r.value());
                        if (r.topic().equals(topicReq)) reqs.add(n); else resps.add(n);
                    } catch (Exception ex) {
                        System.err.println("[CH-SINK] bad JSON: " + ex.getMessage());
                    }
                }
                System.out.printf("[CH-SINK] polled: total=%d req=%d resp=%d%n",
                        records.count(), reqs.size(), resps.size());

                if (!reqs.isEmpty()) {
                    try (PreparedStatement ps = c.prepareStatement(INS_REQ)) {
                        for (JsonNode n : reqs) {
                            ps.setLong(1, n.path("ts").asLong(System.currentTimeMillis()));
                            ps.setString(2, n.path("req_uuid").asText());
                            ps.setString(3, n.path("openrtb_id").asText(""));
                            ps.setInt(4, n.path("tmax").asInt(0));
                            ps.setInt(5, n.path("fanout").asInt(0));
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (Exception ex) {
                        System.err.println("[CH-SINK] INSERT rtb_requests failed: " + ex);
                    }
                }
                if (!resps.isEmpty()) {
                    try (PreparedStatement ps = c.prepareStatement(INS_RESP)) {
                        for (JsonNode n : resps) {
                            ps.setLong(1, n.path("ts").asLong(System.currentTimeMillis()));
                            ps.setString(2, n.path("req_uuid").asText());
                            ps.setString(3, n.path("target").asText(""));
                            ps.setInt(4, n.path("status_code").asInt(0));
                            ps.setInt(5, n.path("duration_ms").asInt(0));
                            ps.setInt(6, n.path("dropped").asInt(0));
                            ps.setString(7, n.path("resp_body").asText(""));
                            ps.addBatch();
                        }
                        ps.executeBatch();
                    } catch (Exception ex) {
                        System.err.println("[CH-SINK] INSERT rtb_responses failed: " + ex);
                    }
                }

                consumer.commitSync();
            }
        } catch (Exception e) {
            System.err.println("[CH-SINK] loop error: " + e);
        } finally {
            try { consumer.close(); } catch (Exception ignore) {}
        }
    }

    private void initTablesAndPing() throws Exception {
        try (Connection c = ds.getConnection()) {
            // ping server version
            try (ResultSet rs = c.createStatement().executeQuery("SELECT version()")) {
                if (rs.next()) System.out.println("[CH] server version: " + rs.getString(1));
            }
            c.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS rtb_requests (
                  ts         DateTime64(3) DEFAULT now64(3),
                  req_uuid   UUID,
                  openrtb_id String,
                  tmax       UInt16,
                  fanout     UInt8
                ) ENGINE = MergeTree ORDER BY (req_uuid, ts)
            """);
            c.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS rtb_responses (
                  ts          DateTime64(3) DEFAULT now64(3),
                  req_uuid    UUID,
                  target      String,
                  status_code UInt16,
                  duration_ms UInt32,
                  dropped     UInt8,
                  resp_body   String
                ) ENGINE = MergeTree ORDER BY (req_uuid, ts)
            """);
        }
    }

    @PreDestroy
    void stop() { running.set(false); }
}