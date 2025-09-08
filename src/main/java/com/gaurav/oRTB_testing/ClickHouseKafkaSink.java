package com.gaurav.oRTB_testing;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

    private static final int INSERT_RETRIES = 3;
    private static final long RETRY_SLEEP_MS = 50L;

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
            @Value("${clickhouse.sinkBatch:1000}") int maxBatch   // keep it modest
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
        // optional timeouts – harmless if driver ignores
        props.setProperty("socket_timeout", "30000");
        props.setProperty("max_execution_time", "30");
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
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        p.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        // slightly shorter to detect slow inserts & re-poll
        p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000");

        consumer = new KafkaConsumer<>(p);
        consumer.subscribe(List.of(topicReq, topicResp));
        running.set(true);
        vexec.execute(this::loop);
        System.out.println("[CH-SINK] started; consuming " + topicReq + ", " + topicResp +
                " from " + bootstrap + " as group " + groupId);
    }

    private void loop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) {
                    continue;
                }

                // Partition by topic and track max offsets we can safely commit per topic/partition
                List<JsonNode> reqs = new ArrayList<>(Math.min(maxBatch, records.count()));
                List<JsonNode> resps = new ArrayList<>(Math.min(maxBatch, records.count()));
                Map<TopicPartition, Long> reqMaxOffset = new HashMap<>();
                Map<TopicPartition, Long> respMaxOffset = new HashMap<>();

                int takenReq = 0, takenResp = 0;

                // Requests
                for (ConsumerRecord<String, String> r : records.records(topicReq)) {
                    if (takenReq >= maxBatch) break; // soft cap per poll
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    try {
                        JsonNode n = M.readTree(r.value());
                        reqs.add(n);
                    } catch (Exception ex) {
                        // bad JSON: skip but still allow commit past this record
                        System.err.println("[CH-SINK] bad JSON (req): " + ex.getMessage());
                    } finally {
                        reqMaxOffset.merge(tp, r.offset(), Math::max);
                        takenReq++;
                    }
                }

                // Responses
                for (ConsumerRecord<String, String> r : records.records(topicResp)) {
                    if (takenResp >= maxBatch) break; // soft cap per poll
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    try {
                        JsonNode n = M.readTree(r.value());
                        resps.add(n);
                    } catch (Exception ex) {
                        System.err.println("[CH-SINK] bad JSON (resp): " + ex.getMessage());
                    } finally {
                        respMaxOffset.merge(tp, r.offset(), Math::max);
                        takenResp++;
                    }
                }

                System.out.printf(
                        Locale.ROOT,
                        "[CH-SINK] polled: total=%d req(batch)=%d resp(batch)=%d%n",
                        records.count(), reqs.size(), resps.size()
                );

                boolean okReq = true;
                boolean okResp = true;

                // Acquire a fresh connection per batch – robust against keepalive resets
                try (Connection c = ds.getConnection()) {
                    if (!reqs.isEmpty()) {
                        okReq = insertReqs(c, reqs);
                    }
                    if (!resps.isEmpty()) {
                        okResp = insertResps(c, resps);
                    }
                } catch (Exception e) {
                    System.err.println("[CH-SINK] connection error: " + e);
                    okReq = okReq && false;
                    okResp = okResp && false;
                }

                // Commit only what we successfully wrote
                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                if (okReq && !reqMaxOffset.isEmpty()) {
                    for (Map.Entry<TopicPartition, Long> e : reqMaxOffset.entrySet()) {
                        commitMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
                    }
                }
                if (okResp && !respMaxOffset.isEmpty()) {
                    for (Map.Entry<TopicPartition, Long> e : respMaxOffset.entrySet()) {
                        commitMap.put(e.getKey(), new OffsetAndMetadata(e.getValue() + 1));
                    }
                }

                if (!commitMap.isEmpty()) {
                    consumer.commitSync(commitMap);
                }

                // If either insert failed, small backoff to avoid hot-looping on CH errors
                if (!(okReq && okResp)) {
                    Thread.sleep(100);
                }
            } catch (WakeupException ex) {
                // shutting down
                break;
            } catch (Exception e) {
                // log and keep going (do not commit anything on generic failure)
                System.err.println("[CH-SINK] loop iteration error: " + e);
                try { Thread.sleep(100); } catch (InterruptedException ignore) {}
            }
        }

        try { consumer.close(); } catch (Exception ignore) {}
    }

    private boolean insertReqs(Connection c, List<JsonNode> reqs) {
        for (int attempt = 1; attempt <= INSERT_RETRIES; attempt++) {
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
                return true;
            } catch (Exception ex) {
                System.err.println("[CH-SINK] INSERT rtb_requests failed (attempt " + attempt + "): " + ex);
                sleepQuiet(RETRY_SLEEP_MS);
            }
        }
        return false;
    }

    private boolean insertResps(Connection c, List<JsonNode> resps) {
        for (int attempt = 1; attempt <= INSERT_RETRIES; attempt++) {
            try (PreparedStatement ps = c.prepareStatement(INS_RESP)) {
                for (JsonNode n : resps) {
                    ps.setLong(1, n.path("ts").asLong(System.currentTimeMillis()));
                    ps.setString(2, n.path("req_uuid").asText());
                    ps.setString(3, n.path("target").asText(""));
                    ps.setInt(4, n.path("status_code").asInt(0));
                    ps.setInt(5, n.path("duration_ms").asInt(0));
                    ps.setInt(6, n.path("dropped").asInt(0));
                    // save only the small summary (id + took_ms) if present; else store raw field
                    String body = n.path("resp_body").asText("");
                    ps.setString(7, body);
                    ps.addBatch();
                }
                ps.executeBatch();
                return true;
            } catch (Exception ex) {
                System.err.println("[CH-SINK] INSERT rtb_responses failed (attempt " + attempt + "): " + ex);
                sleepQuiet(RETRY_SLEEP_MS);
            }
        }
        return false;
    }

    private static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignore) {}
    }

    private void initTablesAndPing() throws Exception {
        try (Connection c = ds.getConnection()) {
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
    void stop() {
        running.set(false);
        try { consumer.wakeup(); } catch (Exception ignore) {}
    }
}