package com.gaurav.oRTB_testing;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Config {
    public enum Mode { SIMULATE, HTTP }

    public final int port;
    public final Mode mode;
    public final int outCalls;                 // N fan-out per request
    public final int totalTimeoutMs;           // request budget (e.g., 300)
    public final int perCallTimeoutMs;         // sub-timeout per upstream

    // SIMULATE mode
    public final int simMinMs, simMaxMs;


    // HTTP fan-out targets (when mode=HTTP); if empty, uses stub bidders
    public List<InetSocketAddress> httpTargets;

    // Kafka
    public final String kafkaBootstrap;
    public final String kafkaTopic;
    public final String kafkaAcks;
    public final String kafkaCompression;
    public final int kafkaLingerMs;
    public final int kafkaBatchBytes;

    public static Config fromEnv() {
        Mode mode = Mode.valueOf(env("MODE", "SIMULATE").toUpperCase());
        int out = intval("OUT_CALLS", 5);
        int total = intval("TIMEOUT_MS", 300);
        int perCall = intval("PER_CALL_TIMEOUT_MS", Math.min(220, total));

        int simMin = intval("SIM_MIN_MS", 50);
        int simMax = intval("SIM_MAX_MS", 150);


        String bootstrap = env("KAFKA_BOOTSTRAP", "");
        String topic = env("KAFKA_TOPIC", "rtb_events");
        String acks = env("KAFKA_ACKS", "1");
        String comp = env("KAFKA_COMPRESSION", "lz4");
        int linger = intval("KAFKA_LINGER_MS", 5);
        int batch = intval("KAFKA_BATCH_BYTES", 131072);

        return new Config(
                intval("PORT", 8080), mode, out, total, perCall,
                simMin, simMax,
                 bootstrap, topic, acks, comp, linger, batch);
    }

    public Config(int port, Mode mode, int outCalls, int totalTimeoutMs, int perCallTimeoutMs,
                  int simMinMs, int simMaxMs,
                  String kafkaBootstrap, String kafkaTopic, String kafkaAcks, String kafkaCompression,
                  int kafkaLingerMs, int kafkaBatchBytes) {
        this.port = port; this.mode = mode; this.outCalls = outCalls; this.totalTimeoutMs = totalTimeoutMs;
        this.perCallTimeoutMs = perCallTimeoutMs; this.simMinMs = simMinMs; this.simMaxMs = simMaxMs;
        this.kafkaBootstrap = kafkaBootstrap; this.kafkaTopic = kafkaTopic; this.kafkaAcks = kafkaAcks;
        this.kafkaCompression = kafkaCompression; this.kafkaLingerMs = kafkaLingerMs; this.kafkaBatchBytes = kafkaBatchBytes;
    }

    static String env(String k, String def) { String v = System.getenv(k); return v == null ? def : v; }
    static int intval(String k, int def) { try { return Integer.parseInt(env(k, String.valueOf(def))); } catch (Exception e) { return def; } }
    static boolean bool(String k, boolean def) { String v = env(k, String.valueOf(def)); return v.equalsIgnoreCase("true") || v.equals("1"); }
}