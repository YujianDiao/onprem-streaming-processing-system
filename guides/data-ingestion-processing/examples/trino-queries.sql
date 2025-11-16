-- ============================================
-- Trino Query Examples for Data Lakehouse
-- ============================================
-- Connect to Trino CLI first: make shell-trino
-- Then copy/paste these queries

-- ============================================
-- 1. BASIC EXPLORATION
-- ============================================

-- Show all available catalogs
SHOW CATALOGS;

-- Show schemas in Iceberg catalog
SHOW SCHEMAS IN iceberg;

-- List tables in bronze layer
SHOW TABLES IN iceberg.bronze;

-- Describe table structure
DESCRIBE iceberg.bronze.transactions;

-- Show table creation DDL
SHOW CREATE TABLE iceberg.bronze.transactions;


-- ============================================
-- 2. SIMPLE QUERIES
-- ============================================

-- Count total transactions
SELECT COUNT(*) as total_transactions
FROM iceberg.bronze.transactions;

-- View latest 10 transactions
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.bronze.transactions
ORDER BY transaction_timestamp DESC
LIMIT 10;

-- Check data freshness
SELECT
    MAX(transaction_timestamp) as latest_transaction,
    MIN(transaction_timestamp) as earliest_transaction,
    COUNT(*) as total_records,
    CURRENT_TIMESTAMP - MAX(transaction_timestamp) as data_lag
FROM iceberg.bronze.transactions;


-- ============================================
-- 3. AGGREGATIONS & ANALYTICS
-- ============================================

-- Total amount by transaction type
SELECT
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM iceberg.bronze.transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Daily transaction summary
SELECT
    date_partition,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_revenue,
    AVG(amount) as avg_transaction_value,
    COUNT(DISTINCT account_id) as unique_accounts
FROM iceberg.bronze.transactions
GROUP BY date_partition
ORDER BY date_partition DESC;

-- Hourly transaction patterns
SELECT
    date_trunc('hour', transaction_timestamp) as hour,
    COUNT(*) as txn_count,
    SUM(amount) as hourly_revenue,
    AVG(amount) as avg_amount
FROM iceberg.bronze.transactions
WHERE date_partition >= CURRENT_DATE
GROUP BY 1
ORDER BY 1 DESC;

-- Top merchants by revenue
SELECT
    merchant,
    COUNT(*) as transaction_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction
FROM iceberg.bronze.transactions
WHERE merchant IS NOT NULL
GROUP BY merchant
ORDER BY total_revenue DESC
LIMIT 20;


-- ============================================
-- 4. FILTERING & SEARCH
-- ============================================

-- High-value transactions (>$10,000)
SELECT
    transaction_id,
    account_id,
    amount,
    merchant,
    transaction_timestamp
FROM iceberg.bronze.transactions
WHERE amount > 10000
ORDER BY amount DESC;

-- Transactions by specific account
SELECT *
FROM iceberg.bronze.transactions
WHERE account_id = 'ACC1234'
ORDER BY transaction_timestamp DESC;

-- Transactions in last hour
SELECT
    COUNT(*) as recent_count,
    SUM(amount) as recent_total,
    AVG(amount) as recent_avg
FROM iceberg.bronze.transactions
WHERE transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Withdrawals and transfers only
SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total
FROM iceberg.bronze.transactions
WHERE transaction_type IN ('WITHDRAWAL', 'TRANSFER')
GROUP BY transaction_type;


-- ============================================
-- 5. ADVANCED ANALYTICS
-- ============================================

-- Customer transaction patterns
SELECT
    account_id,
    COUNT(*) as txn_count,
    SUM(amount) as total_spent,
    AVG(amount) as avg_spent,
    COUNT(DISTINCT merchant) as unique_merchants,
    MAX(amount) as largest_transaction
FROM iceberg.bronze.transactions
GROUP BY account_id
HAVING COUNT(*) > 10
ORDER BY total_spent DESC
LIMIT 20;

-- Fraud analysis
SELECT
    transaction_type,
    COUNT(*) as total,
    SUM(CASE WHEN amount > 10000 THEN 1 ELSE 0 END) as high_value_count,
    CAST(SUM(CASE WHEN amount > 10000 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 as high_value_pct
FROM iceberg.bronze.transactions
GROUP BY transaction_type
ORDER BY high_value_pct DESC;

-- Percentile analysis
SELECT
    transaction_type,
    approx_percentile(amount, 0.25) as p25,
    approx_percentile(amount, 0.50) as median,
    approx_percentile(amount, 0.75) as p75,
    approx_percentile(amount, 0.95) as p95,
    approx_percentile(amount, 0.99) as p99
FROM iceberg.bronze.transactions
GROUP BY transaction_type;

-- Moving average (last 100 transactions per type)
WITH numbered_txns AS (
    SELECT
        transaction_type,
        amount,
        transaction_timestamp,
        ROW_NUMBER() OVER (PARTITION BY transaction_type ORDER BY transaction_timestamp DESC) as rn
    FROM iceberg.bronze.transactions
)
SELECT
    transaction_type,
    AVG(amount) as avg_last_100
FROM numbered_txns
WHERE rn <= 100
GROUP BY transaction_type;


-- ============================================
-- 6. CREATE NEW TABLES (SILVER LAYER)
-- ============================================

-- Create Silver schema if not exists
CREATE SCHEMA IF NOT EXISTS iceberg.silver;

-- Create validated transactions table
CREATE TABLE IF NOT EXISTS iceberg.silver.validated_transactions AS
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    date_partition
FROM iceberg.bronze.transactions
WHERE status = 'PENDING'
  AND amount > 0
  AND amount < 100000
  AND transaction_timestamp IS NOT NULL;

-- Create high-value transactions table
CREATE TABLE IF NOT EXISTS iceberg.silver.high_value_transactions AS
SELECT *
FROM iceberg.bronze.transactions
WHERE amount > 10000;


-- ============================================
-- 7. CREATE AGGREGATE TABLES (GOLD LAYER)
-- ============================================

-- Create Gold schema if not exists
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

-- Create daily summary table
CREATE TABLE IF NOT EXISTS iceberg.gold.daily_summary AS
SELECT
    date_partition,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM iceberg.bronze.transactions
GROUP BY date_partition, transaction_type;

-- Create merchant summary table
CREATE TABLE IF NOT EXISTS iceberg.gold.merchant_summary AS
SELECT
    merchant,
    COUNT(*) as total_transactions,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction,
    MAX(transaction_timestamp) as last_transaction
FROM iceberg.bronze.transactions
WHERE merchant IS NOT NULL
GROUP BY merchant;


-- ============================================
-- 8. ICEBERG TIME TRAVEL
-- ============================================

-- View table snapshots
SELECT
    snapshot_id,
    parent_id,
    operation,
    committed_at,
    summary
FROM iceberg.bronze.transactions$snapshots
ORDER BY committed_at DESC;

-- Query historical data (replace timestamp with actual time)
SELECT COUNT(*)
FROM iceberg.bronze.transactions
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00';

-- Compare current vs historical
WITH current_data AS (
    SELECT COUNT(*) as current_count
    FROM iceberg.bronze.transactions
),
historical_data AS (
    SELECT COUNT(*) as historical_count
    FROM iceberg.bronze.transactions
    FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 10:00:00'
)
SELECT
    current_count,
    historical_count,
    current_count - historical_count as new_records
FROM current_data, historical_data;


-- ============================================
-- 9. ICEBERG METADATA QUERIES
-- ============================================

-- View table files
SELECT
    file_path,
    file_format,
    record_count,
    file_size_in_bytes / 1024 / 1024 as size_mb,
    partition
FROM iceberg.bronze.transactions$files
ORDER BY record_count DESC
LIMIT 10;

-- View partitions
SELECT
    partition,
    record_count,
    file_count,
    spec_id
FROM iceberg.bronze.transactions$partitions
ORDER BY record_count DESC;

-- View table history
SELECT
    made_current_at,
    snapshot_id,
    parent_id,
    is_current_ancestor
FROM iceberg.bronze.transactions$history
ORDER BY made_current_at DESC;


-- ============================================
-- 10. PERFORMANCE OPTIMIZATION
-- ============================================

-- Check partition distribution
SELECT
    date_partition,
    COUNT(*) as record_count
FROM iceberg.bronze.transactions
GROUP BY date_partition
ORDER BY date_partition DESC;

-- Analyze query performance
EXPLAIN SELECT
    transaction_type,
    COUNT(*) as count,
    SUM(amount) as total
FROM iceberg.bronze.transactions
WHERE date_partition >= DATE '2024-01-01'
GROUP BY transaction_type;

-- Table statistics
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT account_id) as unique_accounts,
    COUNT(DISTINCT transaction_type) as unique_types,
    COUNT(DISTINCT date_partition) as partition_count,
    MIN(transaction_timestamp) as earliest,
    MAX(transaction_timestamp) as latest
FROM iceberg.bronze.transactions;


-- ============================================
-- 11. DATA QUALITY CHECKS
-- ============================================

-- Check for nulls
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_txn_id,
    SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END) as null_account,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amount,
    SUM(CASE WHEN merchant IS NULL THEN 1 ELSE 0 END) as null_merchant
FROM iceberg.bronze.transactions;

-- Check for invalid amounts
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN amount <= 0 THEN 1 ELSE 0 END) as negative_or_zero,
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END) as suspiciously_high
FROM iceberg.bronze.transactions;

-- Check for duplicates
SELECT
    transaction_id,
    COUNT(*) as occurrence_count
FROM iceberg.bronze.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;


-- ============================================
-- 12. INCREMENTAL UPDATES
-- ============================================

-- Insert new data into Silver table (incremental)
INSERT INTO iceberg.silver.validated_transactions
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    currency,
    merchant,
    transaction_timestamp,
    date_partition
FROM iceberg.bronze.transactions
WHERE date_partition = CURRENT_DATE
  AND transaction_id NOT IN (
      SELECT transaction_id FROM iceberg.silver.validated_transactions
  );

-- Update Gold daily summary (merge new data)
DELETE FROM iceberg.gold.daily_summary WHERE date_partition = CURRENT_DATE;

INSERT INTO iceberg.gold.daily_summary
SELECT
    date_partition,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM iceberg.bronze.transactions
WHERE date_partition = CURRENT_DATE
GROUP BY date_partition, transaction_type;


-- ============================================
-- 13. USEFUL VIEWS
-- ============================================

-- Create a view for recent transactions
CREATE OR REPLACE VIEW iceberg.bronze.recent_transactions AS
SELECT *
FROM iceberg.bronze.transactions
WHERE transaction_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Query the view
SELECT COUNT(*) FROM iceberg.bronze.recent_transactions;


-- ============================================
-- TIPS & BEST PRACTICES
-- ============================================

-- 1. Always use partition pruning for better performance
--    WHERE date_partition >= DATE '2024-01-01'

-- 2. Use LIMIT when exploring large tables
--    SELECT * FROM table LIMIT 100;

-- 3. Check data freshness before running analytics
--    SELECT MAX(transaction_timestamp) FROM table;

-- 4. Use EXPLAIN to understand query plans
--    EXPLAIN SELECT ...;

-- 5. Create materialized views (tables) for frequently used aggregations

-- 6. Use time travel to analyze changes over time

-- 7. Monitor table growth with metadata queries
--    SELECT * FROM table$files;

-- 8. Partition tables by date for time-series data

-- 9. Use appropriate data types (DECIMAL for money, not DOUBLE)

-- 10. Test queries on small datasets first (LIMIT 1000)
