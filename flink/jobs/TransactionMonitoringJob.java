package com.banking.flink.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Real-Time Transaction Monitoring and Alerting Pipeline
 *
 * Banking Use Cases:
 * 1. Transaction Volume Monitoring - Alert on unusual transaction volumes
 * 2. Large Transaction Alerts - Immediate alerts for high-value transactions
 * 3. Account Activity Monitoring - Track unusual account behavior
 * 4. Threshold Breaches - Alert when accounts exceed daily/hourly limits
 * 5. Transaction Pattern Analysis - Detect unusual transaction patterns
 * 6. Real-time KPI Calculation - Calculate real-time banking metrics
 *
 * Features:
 * - Sliding window aggregations for trend analysis
 * - Stateful processing for tracking account limits
 * - Multi-level alerting (INFO, WARNING, CRITICAL)
 * - Real-time metrics computation
 */
public class TransactionMonitoringJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Thresholds
    private static final double LARGE_TRANSACTION_THRESHOLD = 50000.0;
    private static final double DAILY_LIMIT = 100000.0;
    private static final int HIGH_VOLUME_THRESHOLD = 20;  // transactions per hour

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing every 30 seconds
        env.enableCheckpointing(30000);
        env.setParallelism(2);

        // Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setTopics("banking.transactions.raw")
                .setGroupId("flink-transaction-monitoring")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<Transaction> transactions = rawStream
                .map(new JsonToTransactionMapper())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((txn, ts) -> txn.getTimestamp())
                );

        // 1. LARGE TRANSACTION ALERTS - Immediate processing
        DataStream<Alert> largeTransactionAlerts = transactions
                .filter(txn -> txn.getAmount() >= LARGE_TRANSACTION_THRESHOLD)
                .map(txn -> new Alert(
                        txn.getAccountId(),
                        "LARGE_TRANSACTION",
                        String.format("Large transaction detected: $%.2f %s at %s",
                                     txn.getAmount(), txn.getTransactionType(), txn.getMerchant()),
                        "CRITICAL",
                        txn.getAmount(),
                        System.currentTimeMillis()
                ));

        // 2. TRANSACTION VOLUME MONITORING - Hourly sliding window
        DataStream<Alert> volumeAlerts = transactions
                .keyBy(Transaction::getAccountId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new TransactionVolumeAggregator(), new VolumeAlertProcessWindowFunction());

        // 3. ACCOUNT ACTIVITY MONITORING - Stateful processing for daily limits
        DataStream<Alert> limitAlerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new DailyLimitMonitorFunction());

        // 4. TRANSACTION PATTERN ANALYSIS - Detect unusual patterns
        DataStream<Alert> patternAlerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new TransactionPatternAnalyzer());

        // 5. REAL-TIME KPIs - Calculate banking metrics every 5 minutes
        DataStream<BankingMetrics> realtimeMetrics = transactions
                .keyBy(txn -> "global")  // Global aggregation
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new BankingMetricsAggregator());

        // Merge all alerts
        DataStream<Alert> allAlerts = largeTransactionAlerts
                .union(volumeAlerts)
                .union(limitAlerts)
                .union(patternAlerts);

        // Print alerts to console
        allAlerts.print();
        realtimeMetrics.print();

        // Sink alerts to Kafka
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.monitoring.alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        allAlerts
                .map(alert -> objectMapper.writeValueAsString(alert))
                .sinkTo(alertSink);

        // Sink metrics to Kafka
        KafkaSink<String> metricsSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.metrics.realtime")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        realtimeMetrics
                .map(metrics -> objectMapper.writeValueAsString(metrics))
                .sinkTo(metricsSink);

        env.execute("Transaction Monitoring and Alerting");
    }

    // ==================== POJOs ====================

    public static class Transaction {
        private String transactionId;
        private long timestamp;
        private String accountId;
        private String transactionType;
        private double amount;
        private String currency;
        private String merchant;
        private String status;

        // Getters and setters
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public String getTransactionType() { return transactionType; }
        public void setTransactionType(String transactionType) { this.transactionType = transactionType; }

        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }

        public String getCurrency() { return currency; }
        public void setCurrency(String currency) { this.currency = currency; }

        public String getMerchant() { return merchant; }
        public void setMerchant(String merchant) { this.merchant = merchant; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }

    public static class Alert {
        private String accountId;
        private String alertType;
        private String message;
        private String severity;
        private double value;
        private long timestamp;

        public Alert() {}

        public Alert(String accountId, String alertType, String message,
                    String severity, double value, long timestamp) {
            this.accountId = accountId;
            this.alertType = alertType;
            this.message = message;
            this.severity = severity;
            this.value = value;
            this.timestamp = timestamp;
        }

        // Getters and setters
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public String getAlertType() { return alertType; }
        public void setAlertType(String alertType) { this.alertType = alertType; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }

        public double getValue() { return value; }
        public void setValue(double value) { this.value = value; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class BankingMetrics {
        private long windowStart;
        private long windowEnd;
        private long totalTransactions;
        private double totalVolume;
        private double avgTransactionAmount;
        private double maxTransactionAmount;
        private long fraudulentCount;
        private double fraudRate;

        // Getters and setters
        public long getWindowStart() { return windowStart; }
        public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

        public long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

        public long getTotalTransactions() { return totalTransactions; }
        public void setTotalTransactions(long totalTransactions) { this.totalTransactions = totalTransactions; }

        public double getTotalVolume() { return totalVolume; }
        public void setTotalVolume(double totalVolume) { this.totalVolume = totalVolume; }

        public double getAvgTransactionAmount() { return avgTransactionAmount; }
        public void setAvgTransactionAmount(double avgTransactionAmount) {
            this.avgTransactionAmount = avgTransactionAmount;
        }

        public double getMaxTransactionAmount() { return maxTransactionAmount; }
        public void setMaxTransactionAmount(double maxTransactionAmount) {
            this.maxTransactionAmount = maxTransactionAmount;
        }

        public long getFraudulentCount() { return fraudulentCount; }
        public void setFraudulentCount(long fraudulentCount) { this.fraudulentCount = fraudulentCount; }

        public double getFraudRate() { return fraudRate; }
        public void setFraudRate(double fraudRate) { this.fraudRate = fraudRate; }
    }

    // ==================== Functions ====================

    public static class JsonToTransactionMapper implements MapFunction<String, Transaction> {
        @Override
        public Transaction map(String json) throws Exception {
            JsonNode node = objectMapper.readTree(json);

            Transaction txn = new Transaction();
            txn.setTransactionId(node.get("transaction_id").asText());
            txn.setTimestamp(parseTimestamp(node.get("timestamp").asText()));
            txn.setAccountId(node.get("account_id").asText());
            txn.setTransactionType(node.get("transaction_type").asText());
            txn.setAmount(node.get("amount").asDouble());
            txn.setCurrency(node.get("currency").asText());
            txn.setMerchant(node.has("merchant") && !node.get("merchant").isNull() ?
                           node.get("merchant").asText() : "N/A");
            txn.setStatus(node.get("status").asText());

            return txn;
        }

        private long parseTimestamp(String timestamp) {
            try {
                return java.time.Instant.parse(timestamp).toEpochMilli();
            } catch (Exception e) {
                return System.currentTimeMillis();
            }
        }
    }

    /**
     * Aggregate transaction volume per account
     */
    public static class TransactionVolumeAggregator
            implements AggregateFunction<Transaction, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Transaction txn, Tuple2<String, Integer> acc) {
            return new Tuple2<>(txn.getAccountId(), acc.f1 + 1);
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }

    /**
     * Process volume aggregates and generate alerts
     */
    public static class VolumeAlertProcessWindowFunction
            implements org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                Tuple2<String, Integer>, Alert, String,
                org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void process(String accountId,
                          Context context,
                          Iterable<Tuple2<String, Integer>> elements,
                          Collector<Alert> out) {
            Tuple2<String, Integer> result = elements.iterator().next();
            int count = result.f1;

            if (count > HIGH_VOLUME_THRESHOLD) {
                out.collect(new Alert(
                    accountId,
                    "HIGH_VOLUME",
                    String.format("Unusually high transaction volume: %d transactions in 1 hour", count),
                    "WARNING",
                    count,
                    System.currentTimeMillis()
                ));
            }
        }
    }

    /**
     * Monitor daily limits using stateful processing
     */
    public static class DailyLimitMonitorFunction
            extends KeyedProcessFunction<String, Transaction, Alert> {

        private transient ValueState<Double> dailyTotal;
        private transient ValueState<Long> lastResetTime;

        @Override
        public void open(Configuration parameters) {
            dailyTotal = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dailyTotal", Double.class, 0.0)
            );
            lastResetTime = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastResetTime", Long.class, 0L)
            );
        }

        @Override
        public void processElement(Transaction txn, Context ctx, Collector<Alert> out) throws Exception {
            long currentTime = System.currentTimeMillis();
            long lastReset = lastResetTime.value();

            // Reset daily total if it's a new day
            if (currentTime - lastReset > 86400000) {  // 24 hours in milliseconds
                dailyTotal.update(0.0);
                lastResetTime.update(currentTime);
            }

            // Update daily total
            double total = dailyTotal.value() + txn.getAmount();
            dailyTotal.update(total);

            // Check if daily limit exceeded
            if (total > DAILY_LIMIT) {
                out.collect(new Alert(
                    txn.getAccountId(),
                    "DAILY_LIMIT_EXCEEDED",
                    String.format("Daily transaction limit exceeded: $%.2f (limit: $%.2f)",
                                 total, DAILY_LIMIT),
                    "CRITICAL",
                    total,
                    currentTime
                ));
            } else if (total > DAILY_LIMIT * 0.8) {
                // Warning at 80% of limit
                out.collect(new Alert(
                    txn.getAccountId(),
                    "DAILY_LIMIT_WARNING",
                    String.format("Approaching daily limit: $%.2f (80%% of $%.2f)",
                                 total, DAILY_LIMIT),
                    "WARNING",
                    total,
                    currentTime
                ));
            }
        }
    }

    /**
     * Analyze transaction patterns
     */
    public static class TransactionPatternAnalyzer
            extends KeyedProcessFunction<String, Transaction, Alert> {

        private transient ValueState<String> lastTransactionType;
        private transient ValueState<Integer> sameTypeCount;

        @Override
        public void open(Configuration parameters) {
            lastTransactionType = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastType", String.class, "")
            );
            sameTypeCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("sameTypeCount", Integer.class, 0)
            );
        }

        @Override
        public void processElement(Transaction txn, Context ctx, Collector<Alert> out) throws Exception {
            String lastType = lastTransactionType.value();
            int count = sameTypeCount.value();

            if (lastType.equals(txn.getTransactionType())) {
                count++;
            } else {
                count = 1;
            }

            // Alert if same transaction type repeated 10 times
            if (count >= 10) {
                out.collect(new Alert(
                    txn.getAccountId(),
                    "UNUSUAL_PATTERN",
                    String.format("Unusual pattern: %d consecutive %s transactions",
                                 count, txn.getTransactionType()),
                    "WARNING",
                    count,
                    System.currentTimeMillis()
                ));
                count = 0;  // Reset
            }

            lastTransactionType.update(txn.getTransactionType());
            sameTypeCount.update(count);
        }
    }

    /**
     * Aggregate real-time banking metrics
     */
    public static class BankingMetricsAggregator
            implements AggregateFunction<Transaction, BankingMetrics, BankingMetrics> {

        @Override
        public BankingMetrics createAccumulator() {
            return new BankingMetrics();
        }

        @Override
        public BankingMetrics add(Transaction txn, BankingMetrics acc) {
            acc.setTotalTransactions(acc.getTotalTransactions() + 1);
            acc.setTotalVolume(acc.getTotalVolume() + txn.getAmount());

            if (txn.getAmount() > acc.getMaxTransactionAmount()) {
                acc.setMaxTransactionAmount(txn.getAmount());
            }

            return acc;
        }

        @Override
        public BankingMetrics getResult(BankingMetrics acc) {
            if (acc.getTotalTransactions() > 0) {
                acc.setAvgTransactionAmount(acc.getTotalVolume() / acc.getTotalTransactions());
            }
            if (acc.getTotalTransactions() > 0) {
                acc.setFraudRate((double) acc.getFraudulentCount() / acc.getTotalTransactions() * 100);
            }
            return acc;
        }

        @Override
        public BankingMetrics merge(BankingMetrics a, BankingMetrics b) {
            BankingMetrics merged = new BankingMetrics();
            merged.setTotalTransactions(a.getTotalTransactions() + b.getTotalTransactions());
            merged.setTotalVolume(a.getTotalVolume() + b.getTotalVolume());
            merged.setMaxTransactionAmount(Math.max(a.getMaxTransactionAmount(), b.getMaxTransactionAmount()));
            merged.setFraudulentCount(a.getFraudulentCount() + b.getFraudulentCount());
            return merged;
        }
    }
}
