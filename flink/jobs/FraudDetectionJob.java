package com.banking.flink.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Real-Time Fraud Detection Pipeline
 *
 * Detects fraudulent transactions using:
 * 1. Rule-based fraud detection (high amounts, suspicious patterns)
 * 2. Complex Event Processing (CEP) for detecting fraud patterns
 * 3. Velocity checks (multiple transactions in short time)
 * 4. Geographic anomaly detection
 *
 * Banking Use Cases:
 * - Detect transactions above $10,000 in short timeframes
 * - Identify multiple failed transactions followed by a successful one
 * - Detect transactions from unusual geographic locations
 * - Monitor rapid-fire transactions (card testing)
 */
public class FraudDetectionJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance (every 10 seconds)
        env.enableCheckpointing(10000);

        // Configure parallelism
        env.setParallelism(2);

        // Kafka source configuration
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setTopics("banking.transactions.raw")
                .setGroupId("flink-fraud-detection")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Read from Kafka
        DataStream<String> rawStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse JSON and extract transaction data
        DataStream<Transaction> transactions = rawStream
                .map(new JsonToTransactionMapper())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((transaction, timestamp) -> transaction.getTimestamp())
                );

        // 1. RULE-BASED FRAUD DETECTION
        DataStream<FraudAlert> ruleBasedFraud = transactions
                .filter(new RuleBasedFraudFilter())
                .map(new TransactionToFraudAlertMapper("RULE_BASED"));

        // 2. VELOCITY CHECK - Multiple transactions from same account in 1 minute
        DataStream<FraudAlert> velocityFraud = transactions
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new VelocityCheckWindowFunction());

        // 3. COMPLEX EVENT PROCESSING - Pattern: 3 failed transactions followed by 1 success
        Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("failed_attempts")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return "FAILED".equals(transaction.getStatus());
                    }
                })
                .times(3)
                .followedBy("successful")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction transaction) {
                        return "SUCCESS".equals(transaction.getStatus()) &&
                               transaction.getAmount() > 1000.0;
                    }
                })
                .within(Time.minutes(5));

        PatternStream<Transaction> patternStream = CEP.pattern(
                transactions.keyBy(Transaction::getAccountId),
                fraudPattern
        );

        DataStream<FraudAlert> cepFraud = patternStream.select(new PatternSelectFunction<Transaction, FraudAlert>() {
            @Override
            public FraudAlert select(Map<String, List<Transaction>> pattern) {
                List<Transaction> failedTxns = pattern.get("failed_attempts");
                Transaction successTxn = pattern.get("successful").get(0);

                return new FraudAlert(
                    successTxn.getTransactionId(),
                    successTxn.getAccountId(),
                    "CEP_PATTERN",
                    "Multiple failed attempts followed by success",
                    "HIGH",
                    successTxn.getAmount(),
                    System.currentTimeMillis()
                );
            }
        });

        // 4. GEOGRAPHIC ANOMALY - Transactions from different locations in short time
        DataStream<FraudAlert> geoFraud = transactions
                .keyBy(Transaction::getAccountId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new GeographicAnomalyWindowFunction());

        // Merge all fraud detection streams
        DataStream<FraudAlert> allFraudAlerts = ruleBasedFraud
                .union(velocityFraud)
                .union(cepFraud)
                .union(geoFraud);

        // Print to console for monitoring
        allFraudAlerts.print();

        // Sink back to Kafka for downstream processing
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.fraud.alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        allFraudAlerts
                .map(alert -> objectMapper.writeValueAsString(alert))
                .sinkTo(kafkaSink);

        // Execute the Flink job
        env.execute("Real-Time Fraud Detection");
    }

    // ==================== Helper Classes ====================

    /**
     * Transaction POJO
     */
    public static class Transaction {
        private String transactionId;
        private long timestamp;
        private String accountId;
        private String transactionType;
        private double amount;
        private String currency;
        private String merchant;
        private Location location;
        private Customer customer;
        private boolean isFraud;
        private String status;
        private Metadata metadata;

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

        public Location getLocation() { return location; }
        public void setLocation(Location location) { this.location = location; }

        public Customer getCustomer() { return customer; }
        public void setCustomer(Customer customer) { this.customer = customer; }

        public boolean isFraud() { return isFraud; }
        public void setFraud(boolean fraud) { isFraud = fraud; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public Metadata getMetadata() { return metadata; }
        public void setMetadata(Metadata metadata) { this.metadata = metadata; }
    }

    public static class Location {
        private String city;
        private String state;
        private String country;
        private double latitude;
        private double longitude;

        // Getters and setters
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }

        public String getState() { return state; }
        public void setState(String state) { this.state = state; }

        public String getCountry() { return country; }
        public void setCountry(String country) { this.country = country; }

        public double getLatitude() { return latitude; }
        public void setLatitude(double latitude) { this.latitude = latitude; }

        public double getLongitude() { return longitude; }
        public void setLongitude(double longitude) { this.longitude = longitude; }
    }

    public static class Customer {
        private String customerId;
        private String name;
        private String email;
        private String phone;

        // Getters and setters
        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }

        public String getPhone() { return phone; }
        public void setPhone(String phone) { this.phone = phone; }
    }

    public static class Metadata {
        private String deviceType;
        private String ipAddress;
        private String sessionId;

        // Getters and setters
        public String getDeviceType() { return deviceType; }
        public void setDeviceType(String deviceType) { this.deviceType = deviceType; }

        public String getIpAddress() { return ipAddress; }
        public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }

        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    }

    /**
     * Fraud Alert POJO
     */
    public static class FraudAlert {
        private String transactionId;
        private String accountId;
        private String fraudType;
        private String reason;
        private String severity;
        private double amount;
        private long detectedAt;

        public FraudAlert() {}

        public FraudAlert(String transactionId, String accountId, String fraudType,
                         String reason, String severity, double amount, long detectedAt) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.fraudType = fraudType;
            this.reason = reason;
            this.severity = severity;
            this.amount = amount;
            this.detectedAt = detectedAt;
        }

        // Getters and setters
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public String getFraudType() { return fraudType; }
        public void setFraudType(String fraudType) { this.fraudType = fraudType; }

        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }

        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }

        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }

        public long getDetectedAt() { return detectedAt; }
        public void setDetectedAt(long detectedAt) { this.detectedAt = detectedAt; }
    }

    /**
     * JSON to Transaction mapper
     */
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
                           node.get("merchant").asText() : null);
            txn.setFraud(node.get("is_fraud").asBoolean());
            txn.setStatus(node.get("status").asText());

            // Parse location
            if (node.has("location")) {
                JsonNode locNode = node.get("location");
                Location loc = new Location();
                loc.setCity(locNode.get("city").asText());
                loc.setState(locNode.get("state").asText());
                loc.setCountry(locNode.get("country").asText());
                loc.setLatitude(locNode.get("latitude").asDouble());
                loc.setLongitude(locNode.get("longitude").asDouble());
                txn.setLocation(loc);
            }

            // Parse customer
            if (node.has("customer")) {
                JsonNode custNode = node.get("customer");
                Customer cust = new Customer();
                cust.setCustomerId(custNode.get("customer_id").asText());
                cust.setName(custNode.get("name").asText());
                cust.setEmail(custNode.get("email").asText());
                cust.setPhone(custNode.get("phone").asText());
                txn.setCustomer(cust);
            }

            // Parse metadata
            if (node.has("metadata")) {
                JsonNode metaNode = node.get("metadata");
                Metadata meta = new Metadata();
                meta.setDeviceType(metaNode.get("device_type").asText());
                meta.setIpAddress(metaNode.get("ip_address").asText());
                meta.setSessionId(metaNode.get("session_id").asText());
                txn.setMetadata(meta);
            }

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
     * Rule-based fraud filter
     */
    public static class RuleBasedFraudFilter implements FilterFunction<Transaction> {
        @Override
        public boolean filter(Transaction txn) {
            // Detect high-value transactions
            if (txn.getAmount() > 10000.0) {
                return true;
            }

            // Detect transactions marked as fraud by data generator
            if (txn.isFraud()) {
                return true;
            }

            // Detect unusual transaction types with high amounts
            if (("WITHDRAWAL".equals(txn.getTransactionType()) ||
                 "TRANSFER".equals(txn.getTransactionType())) &&
                txn.getAmount() > 5000.0) {
                return true;
            }

            return false;
        }
    }

    /**
     * Transaction to Fraud Alert mapper
     */
    public static class TransactionToFraudAlertMapper implements MapFunction<Transaction, FraudAlert> {
        private final String fraudType;

        public TransactionToFraudAlertMapper(String fraudType) {
            this.fraudType = fraudType;
        }

        @Override
        public FraudAlert map(Transaction txn) {
            String reason = generateReason(txn);
            String severity = txn.getAmount() > 20000.0 ? "CRITICAL" :
                            txn.getAmount() > 10000.0 ? "HIGH" : "MEDIUM";

            return new FraudAlert(
                txn.getTransactionId(),
                txn.getAccountId(),
                fraudType,
                reason,
                severity,
                txn.getAmount(),
                System.currentTimeMillis()
            );
        }

        private String generateReason(Transaction txn) {
            if (txn.getAmount() > 20000.0) {
                return "Critical: Transaction amount exceeds $20,000";
            } else if (txn.getAmount() > 10000.0) {
                return "High: Transaction amount exceeds $10,000";
            } else if (txn.isFraud()) {
                return "Flagged as fraudulent by upstream system";
            } else {
                return "Suspicious transaction pattern detected";
            }
        }
    }

    /**
     * Velocity check window function - placeholder
     * (Full implementation would use WindowFunction with state)
     */
    public static class VelocityCheckWindowFunction
            implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<
                Transaction, FraudAlert, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void apply(String accountId,
                         org.apache.flink.streaming.api.windowing.windows.TimeWindow window,
                         Iterable<Transaction> transactions,
                         org.apache.flink.util.Collector<FraudAlert> out) {
            int count = 0;
            double totalAmount = 0.0;
            Transaction lastTxn = null;

            for (Transaction txn : transactions) {
                count++;
                totalAmount += txn.getAmount();
                lastTxn = txn;
            }

            // Flag if more than 5 transactions in 1 minute
            if (count > 5 && lastTxn != null) {
                out.collect(new FraudAlert(
                    lastTxn.getTransactionId(),
                    accountId,
                    "VELOCITY_CHECK",
                    String.format("%d transactions in 1 minute (total: $%.2f)", count, totalAmount),
                    "HIGH",
                    totalAmount,
                    System.currentTimeMillis()
                ));
            }
        }
    }

    /**
     * Geographic anomaly detection window function - placeholder
     */
    public static class GeographicAnomalyWindowFunction
            implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<
                Transaction, FraudAlert, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void apply(String accountId,
                         org.apache.flink.streaming.api.windowing.windows.TimeWindow window,
                         Iterable<Transaction> transactions,
                         org.apache.flink.util.Collector<FraudAlert> out) {
            String firstCity = null;
            Transaction lastTxn = null;
            boolean anomalyDetected = false;

            for (Transaction txn : transactions) {
                if (txn.getLocation() != null) {
                    if (firstCity == null) {
                        firstCity = txn.getLocation().getCity();
                    } else if (!firstCity.equals(txn.getLocation().getCity())) {
                        // Different cities in 5 minutes - potential fraud
                        anomalyDetected = true;
                    }
                }
                lastTxn = txn;
            }

            if (anomalyDetected && lastTxn != null) {
                out.collect(new FraudAlert(
                    lastTxn.getTransactionId(),
                    accountId,
                    "GEOGRAPHIC_ANOMALY",
                    "Transactions from multiple cities in 5-minute window",
                    "MEDIUM",
                    lastTxn.getAmount(),
                    System.currentTimeMillis()
                ));
            }
        }
    }
}
