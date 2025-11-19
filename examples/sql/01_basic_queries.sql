-- ============================================================================
-- Basic SQL Queries for Data Lakehouse Platform
-- ============================================================================
-- Purpose: Get started with querying transaction data
-- Tool: Trino SQL
-- Run: make shell-trino, then paste these queries
-- ============================================================================

-- Switch to Iceberg catalog
USE iceberg;

-- ============================================================================
-- 1. DATA OVERVIEW
-- ============================================================================

-- Show all available schemas (databases)
SHOW SCHEMAS;

-- Show tables in Bronze layer
SHOW TABLES IN bronze;

-- Describe Bronze transactions table structure
DESCRIBE bronze.transactions;

-- Count total records
SELECT COUNT(*) as total_records
FROM bronze.transactions;

-- Get date range of data
SELECT
    MIN(CAST(timestamp AS DATE)) as earliest_date,
    MAX(CAST(timestamp AS DATE)) as latest_date,
    DATE_DIFF('day', MIN(CAST(timestamp AS DATE)), MAX(CAST(timestamp AS DATE))) as days_of_data
FROM bronze.transactions;

-- ============================================================================
-- 2. SIMPLE AGGREGATIONS
-- ============================================================================

-- Total transaction volume
SELECT
    COUNT(*) as total_transactions,
    COUNT(DISTINCT account_id) as unique_accounts,
    ROUND(SUM(amount), 2) as total_volume,
    ROUND(AVG(amount), 2) as average_transaction,
    ROUND(MIN(amount), 2) as min_transaction,
    ROUND(MAX(amount), 2) as max_transaction
FROM bronze.transactions;

-- Transactions by type
SELECT
    transaction_type,
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM bronze.transactions
GROUP BY transaction_type
ORDER BY count DESC;

-- Transactions by status
SELECT
    status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM bronze.transactions
GROUP BY status;

-- ============================================================================
-- 3. TOP N QUERIES
-- ============================================================================

-- Top 10 merchants by transaction count
SELECT
    merchant,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_revenue,
    ROUND(AVG(amount), 2) as avg_transaction
FROM bronze.transactions
GROUP BY merchant
ORDER BY transactions DESC
LIMIT 10;

-- Top 10 highest value transactions
SELECT
    transaction_id,
    account_id,
    merchant,
    amount,
    transaction_type,
    CAST(timestamp AS TIMESTAMP) as transaction_time,
    is_fraud
FROM bronze.transactions
ORDER BY amount DESC
LIMIT 10;

-- Top 10 most active accounts
SELECT
    account_id,
    customer_name,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount
FROM bronze.transactions
GROUP BY account_id, customer_name
ORDER BY transaction_count DESC
LIMIT 10;

-- ============================================================================
-- 4. FILTERING QUERIES
-- ============================================================================

-- Transactions today
SELECT
    COUNT(*) as todays_transactions,
    ROUND(SUM(amount), 2) as todays_volume
FROM bronze.transactions
WHERE CAST(timestamp AS DATE) = CURRENT_DATE;

-- High-value transactions (over $10,000)
SELECT
    transaction_id,
    account_id,
    amount,
    merchant,
    transaction_type
FROM bronze.transactions
WHERE amount > 10000
ORDER BY amount DESC;

-- Flagged fraud transactions
SELECT
    transaction_id,
    account_id,
    merchant,
    amount,
    CAST(timestamp AS TIMESTAMP) as time,
    location.city as city
FROM bronze.transactions
WHERE is_fraud = true
ORDER BY amount DESC;

-- Transactions by specific merchant
SELECT
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total,
    ROUND(AVG(amount), 2) as average
FROM bronze.transactions
WHERE merchant = 'Amazon';

-- ============================================================================
-- 5. TIME-BASED ANALYSIS
-- ============================================================================

-- Transactions by hour of day
SELECT
    HOUR(CAST(timestamp AS TIMESTAMP)) as hour,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_amount
FROM bronze.transactions
GROUP BY HOUR(CAST(timestamp AS TIMESTAMP))
ORDER BY hour;

-- Transactions by day of week
SELECT
    DAY_OF_WEEK(CAST(timestamp AS TIMESTAMP)) as day_of_week,
    CASE DAY_OF_WEEK(CAST(timestamp AS TIMESTAMP))
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END as day_name,
    COUNT(*) as transactions,
    ROUND(AVG(amount), 2) as avg_amount
FROM bronze.transactions
GROUP BY DAY_OF_WEEK(CAST(timestamp AS TIMESTAMP))
ORDER BY day_of_week;

-- Daily transaction summary
SELECT
    CAST(timestamp AS DATE) as date,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as daily_volume,
    COUNT(DISTINCT account_id) as active_accounts
FROM bronze.transactions
GROUP BY CAST(timestamp AS DATE)
ORDER BY date DESC;

-- ============================================================================
-- 6. GEOGRAPHIC ANALYSIS
-- ============================================================================

-- Transactions by city
SELECT
    location.city,
    COUNT(*) as transactions,
    ROUND(SUM(amount), 2) as total_volume,
    COUNT(DISTINCT account_id) as unique_customers
FROM bronze.transactions
GROUP BY location.city
ORDER BY total_volume DESC;

-- ============================================================================
-- 7. RECENT DATA QUERIES
-- ============================================================================

-- Last 10 transactions
SELECT
    transaction_id,
    account_id,
    merchant,
    amount,
    transaction_type,
    CAST(timestamp AS TIMESTAMP) as time
FROM bronze.transactions
ORDER BY timestamp DESC
LIMIT 10;

-- Transactions in last hour
SELECT
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total
FROM bronze.transactions
WHERE CAST(timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP - INTERVAL '1' HOUR;

-- Transactions in last 24 hours
SELECT
    COUNT(*) as count,
    ROUND(SUM(amount), 2) as total
FROM bronze.transactions
WHERE CAST(timestamp AS TIMESTAMP) > CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

-- ============================================================================
-- 8. DATA FRESHNESS CHECK
-- ============================================================================

-- Check data freshness (how old is newest data?)
SELECT
    MAX(CAST(timestamp AS TIMESTAMP)) as latest_transaction,
    MAX(CAST(ingestion_timestamp AS TIMESTAMP)) as latest_ingestion,
    CURRENT_TIMESTAMP as current_time,
    ROUND(
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(CAST(ingestion_timestamp AS TIMESTAMP)))) / 60,
        2
    ) as lag_minutes
FROM bronze.transactions;

-- ============================================================================
-- 9. SAMPLE DATA
-- ============================================================================

-- Get sample of 10 random transactions
SELECT *
FROM bronze.transactions
ORDER BY RANDOM()
LIMIT 10;

-- ============================================================================
-- Next: Try intermediate_queries.sql for more advanced analysis!
-- ============================================================================
