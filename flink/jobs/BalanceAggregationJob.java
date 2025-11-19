package com.banking.flink.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Real-Time Balance Aggregation Pipeline
 *
 * Banking Use Cases:
 * 1. Real-time Account Balance Calculation
 * 2. Running Balance Maintenance
 * 3. Available Balance Tracking
 * 4. Balance Snapshots (hourly, daily)
 * 5. Low Balance Alerts
 * 6. Overdraft Detection
 * 7. Customer-level Balance Aggregation
 * 8. Transaction Type Aggregation
 *
 * Features:
 * - Stateful balance calculation using Flink's ValueState
 * - Event-time processing for accurate balance computation
 * - Windowed aggregations for periodic snapshots
 * - Low balance and overdraft alerting
 * - Support for deposits, withdrawals, transfers, payments
 */
public class BalanceAggregationJob {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double LOW_BALANCE_THRESHOLD = 100.0;
    private static final double OVERDRAFT_LIMIT = -500.0;
    private static final DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000);
        env.setParallelism(2);

        // Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setTopics("banking.transactions.raw")
                .setGroupId("flink-balance-aggregation")
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

        // 1. REAL-TIME RUNNING BALANCE - Stateful computation
        DataStream<AccountBalance> runningBalances = transactions
                .keyBy(Transaction::getAccountId)
                .process(new RunningBalanceCalculator());

        // 2. BALANCE SNAPSHOTS - Hourly aggregation
        DataStream<BalanceSnapshot> hourlySnapshots = runningBalances
                .keyBy(AccountBalance::getAccountId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new BalanceSnapshotWindowFunction());

        // 3. LOW BALANCE ALERTS
        DataStream<BalanceAlert> lowBalanceAlerts = runningBalances
                .filter(balance -> balance.getCurrentBalance() < LOW_BALANCE_THRESHOLD &&
                                  balance.getCurrentBalance() >= 0)
                .map(balance -> new BalanceAlert(
                        balance.getAccountId(),
                        "LOW_BALANCE",
                        String.format("Low balance warning: $%.2f", balance.getCurrentBalance()),
                        "WARNING",
                        balance.getCurrentBalance(),
                        System.currentTimeMillis()
                ));

        // 4. OVERDRAFT ALERTS
        DataStream<BalanceAlert> overdraftAlerts = runningBalances
                .filter(balance -> balance.getCurrentBalance() < 0)
                .map(balance -> {
                    String severity = balance.getCurrentBalance() < OVERDRAFT_LIMIT ? "CRITICAL" : "WARNING";
                    return new BalanceAlert(
                            balance.getAccountId(),
                            "OVERDRAFT",
                            String.format("Account overdrawn: $%.2f", balance.getCurrentBalance()),
                            severity,
                            balance.getCurrentBalance(),
                            System.currentTimeMillis()
                    );
                });

        // 5. TRANSACTION TYPE AGGREGATION - Per account, per hour
        DataStream<TransactionTypeStats> typeStats = transactions
                .keyBy(txn -> new Tuple3<>(
                        txn.getAccountId(),
                        txn.getTransactionType(),
                        txn.getTimestamp() / 3600000  // Hour bucket
                ))
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new TransactionTypeAggregator());

        // 6. CUSTOMER-LEVEL AGGREGATION - Aggregate across all accounts per customer
        DataStream<CustomerBalance> customerBalances = transactions
                .keyBy(txn -> txn.getCustomerId())
                .process(new CustomerBalanceCalculator());

        // Print outputs
        runningBalances.print().setParallelism(1).name("Running Balances");
        hourlySnapshots.print().setParallelism(1).name("Hourly Snapshots");
        lowBalanceAlerts.print().setParallelism(1).name("Low Balance Alerts");
        overdraftAlerts.print().setParallelism(1).name("Overdraft Alerts");

        // Sink running balances to Kafka
        KafkaSink<String> balanceSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.balances.realtime")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        runningBalances
                .map(balance -> objectMapper.writeValueAsString(balance))
                .sinkTo(balanceSink);

        // Sink alerts to Kafka
        KafkaSink<String> alertSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.balance.alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        DataStream<BalanceAlert> allAlerts = lowBalanceAlerts.union(overdraftAlerts);
        allAlerts
                .map(alert -> objectMapper.writeValueAsString(alert))
                .sinkTo(alertSink);

        // Sink snapshots to Kafka for persistence
        KafkaSink<String> snapshotSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:29092,kafka-2:29092,kafka-3:29092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("banking.balance.snapshots")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        hourlySnapshots
                .map(snapshot -> objectMapper.writeValueAsString(snapshot))
                .sinkTo(snapshotSink);

        env.execute("Real-Time Balance Aggregation");
    }

    // ==================== POJOs ====================

    public static class Transaction {
        private String transactionId;
        private long timestamp;
        private String accountId;
        private String customerId;
        private String transactionType;
        private double amount;
        private String currency;
        private String status;

        // Getters and setters
        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public String getCustomerId() { return customerId; }
        public void setCustomerId(String customerId) { this.customerId = customerId; }

        public String getTransactionType() { return transactionType; }
        public void setTransactionType(String transactionType) { this.transactionType = transactionType; }

        public double getAmount() { return amount; }
        public void setAmount(double amount) { this.amount = amount; }

        public String getCurrency() { return currency; }
        public void setCurrency(String currency) { this.currency = currency; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }

    public static class AccountBalance {
        private String accountId;
        private double currentBalance;
        private double availableBalance;
        private long lastTransactionTime;
        private String lastTransactionId;
        private int transactionCount;

        public AccountBalance() {}

        // Getters and setters
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public double getCurrentBalance() { return currentBalance; }
        public void setCurrentBalance(double currentBalance) { this.currentBalance = currentBalance; }

        public double getAvailableBalance() { return availableBalance; }
        public void setAvailableBalance(double availableBalance) { this.availableBalance = availableBalance; }

        public long getLastTransactionTime() { return lastTransactionTime; }
        public void setLastTransactionTime(long lastTransactionTime) {
            this.lastTransactionTime = lastTransactionTime;
        }

        public String getLastTransactionId() { return lastTransactionId; }
        public void setLastTransactionId(String lastTransactionId) {
            this.lastTransactionId = lastTransactionId;
        }

        public int getTransactionCount() { return transactionCount; }
        public void setTransactionCount(int transactionCount) { this.transactionCount = transactionCount; }
    }

    public static class BalanceSnapshot {
        private String accountId;
        private double openingBalance;
        private double closingBalance;
        private double totalCredits;
        private double totalDebits;
        private int creditCount;
        private int debitCount;
        private long windowStart;
        private long windowEnd;

        // Getters and setters
        public String getAccountId() { return accountId; }
        public void setAccountId(String accountId) { this.accountId = accountId; }

        public double getOpeningBalance() { return openingBalance; }
        public void setOpeningBalance(double openingBalance) { this.openingBalance = openingBalance; }

        public double getClosingBalance() { return closingBalance; }
        public void setClosingBalance(double closingBalance) { this.closingBalance = closingBalance; }

        public double getTotalCredits() { return totalCredits; }
        public void setTotalCredits(double totalCredits) { this.totalCredits = totalCredits; }

        public double getTotalDebits() { return totalDebits; }
        public void setTotalDebits(double totalDebits) { this.totalDebits = totalDebits; }

        public int getCreditCount() { return creditCount; }
        public void setCreditCount(int creditCount) { this.creditCount = creditCount; }

        public int getDebitCount() { return debitCount; }
        public void setDebitCount(int debitCount) { this.debitCount = debitCount; }

        public long getWindowStart() { return windowStart; }
        public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

        public long getWindowEnd() { return windowEnd; }
        public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
    }

    public static class BalanceAlert {
        private String accountId;
        private String alertType;
        private String message;
        private String severity;
        private double balance;
        private long timestamp;

        public BalanceAlert() {}

        public BalanceAlert(String accountId, String alertType, String message,
                           String severity, double balance, long timestamp) {
            this.accountId = accountId;
            this.alertType = alertType;
            this.message = message;
            this.severity = severity;
            this.balance = balance;
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

        public double getBalance() { return balance; }
        public void setBalance(double balance) { this.balance = balance; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    }

    public static class TransactionTypeStats {
        private String accountId;
        private String transactionType;
        private int count;
        private double totalAmount;
        private long windowStart;

        // Getters and setters (omitted for brevity)
    }

    public static class CustomerBalance {
        private String customerId;
        private double totalBalance;
        private int accountCount;
        private long lastUpdated;

        // Getters and setters (omitted for brevity)
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
            txn.setStatus(node.get("status").asText());

            // Extract customer ID
            if (node.has("customer") && node.get("customer").has("customer_id")) {
                txn.setCustomerId(node.get("customer").get("customer_id").asText());
            } else {
                txn.setCustomerId("UNKNOWN");
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
     * Calculate running balance with stateful processing
     */
    public static class RunningBalanceCalculator
            extends KeyedProcessFunction<String, Transaction, AccountBalance> {

        private transient ValueState<AccountBalance> balanceState;

        @Override
        public void open(Configuration parameters) {
            balanceState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("accountBalance", AccountBalance.class)
            );
        }

        @Override
        public void processElement(Transaction txn, Context ctx, Collector<AccountBalance> out) throws Exception {
            AccountBalance balance = balanceState.value();

            if (balance == null) {
                balance = new AccountBalance();
                balance.setAccountId(txn.getAccountId());
                balance.setCurrentBalance(10000.0);  // Starting balance (could be loaded from DB)
                balance.setAvailableBalance(10000.0);
                balance.setTransactionCount(0);
            }

            // Update balance based on transaction type
            double change = calculateBalanceChange(txn);
            balance.setCurrentBalance(balance.getCurrentBalance() + change);
            balance.setAvailableBalance(balance.getAvailableBalance() + change);
            balance.setLastTransactionTime(txn.getTimestamp());
            balance.setLastTransactionId(txn.getTransactionId());
            balance.setTransactionCount(balance.getTransactionCount() + 1);

            balanceState.update(balance);
            out.collect(balance);
        }

        private double calculateBalanceChange(Transaction txn) {
            switch (txn.getTransactionType()) {
                case "DEPOSIT":
                    return txn.getAmount();  // Credit
                case "WITHDRAWAL":
                case "PURCHASE":
                case "PAYMENT":
                    return -txn.getAmount();  // Debit
                case "TRANSFER":
                    // Simplified: assume outgoing transfer
                    return -txn.getAmount();  // Debit
                default:
                    return 0.0;
            }
        }
    }

    /**
     * Create balance snapshots for each window
     */
    public static class BalanceSnapshotWindowFunction
            implements org.apache.flink.streaming.api.functions.windowing.WindowFunction<
                    AccountBalance, BalanceSnapshot, String,
                    org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void apply(String accountId,
                         org.apache.flink.streaming.api.windowing.windows.TimeWindow window,
                         Iterable<AccountBalance> balances,
                         Collector<BalanceSnapshot> out) {

            BalanceSnapshot snapshot = new BalanceSnapshot();
            snapshot.setAccountId(accountId);
            snapshot.setWindowStart(window.getStart());
            snapshot.setWindowEnd(window.getEnd());

            double openingBalance = 0.0;
            double closingBalance = 0.0;
            boolean first = true;

            for (AccountBalance balance : balances) {
                if (first) {
                    openingBalance = balance.getCurrentBalance();
                    first = false;
                }
                closingBalance = balance.getCurrentBalance();
            }

            snapshot.setOpeningBalance(openingBalance);
            snapshot.setClosingBalance(closingBalance);

            out.collect(snapshot);
        }
    }

    /**
     * Aggregate transaction type statistics
     */
    public static class TransactionTypeAggregator
            implements org.apache.flink.api.common.functions.AggregateFunction<
                    Transaction, TransactionTypeStats, TransactionTypeStats> {

        @Override
        public TransactionTypeStats createAccumulator() {
            return new TransactionTypeStats();
        }

        @Override
        public TransactionTypeStats add(Transaction txn, TransactionTypeStats acc) {
            // Implementation details omitted for brevity
            return acc;
        }

        @Override
        public TransactionTypeStats getResult(TransactionTypeStats acc) {
            return acc;
        }

        @Override
        public TransactionTypeStats merge(TransactionTypeStats a, TransactionTypeStats b) {
            // Implementation details omitted for brevity
            return a;
        }
    }

    /**
     * Calculate customer-level balances
     */
    public static class CustomerBalanceCalculator
            extends KeyedProcessFunction<String, Transaction, CustomerBalance> {

        // Implementation details omitted for brevity
        @Override
        public void processElement(Transaction txn, Context ctx, Collector<CustomerBalance> out) {
            // Aggregate across all accounts for this customer
        }
    }
}
