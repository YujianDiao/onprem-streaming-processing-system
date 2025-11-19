-- ============================================================================
-- Intermediate SQL Queries for Data Lakehouse Platform
-- ============================================================================
-- Purpose: Advanced analytics and business insights
-- Prerequisites: Run 01_basic_queries.sql first
-- ============================================================================

USE iceberg;

-- ============================================================================
-- 1. COHORT ANALYSIS
-- ============================================================================

-- Daily cohort: New accounts per day
WITH first_transactions AS (
    SELECT
        account_id,
        MIN(CAST(timestamp AS DATE)) as first_transaction_date
    FROM bronze.transactions
    GROUP BY account_id
)
SELECT
    first_transaction_date as cohort_date,
    COUNT(DISTINCT account_id) as new_accounts
FROM first_transactions
GROUP BY first_transaction_date
ORDER BY cohort_date;

-- Customer lifetime value (CLV)
SELECT
    account_id,
    customer_name,
    MIN(CAST(timestamp AS TIMESTAMP)) as first_transaction,
    MAX(CAST(timestamp AS TIMESTAMP)) as last_transaction,
    COUNT(*) as total_transactions,
    ROUND(SUM(amount), 2) as lifetime_value,
    ROUND(AVG(amount), 2) as avg_transaction_value,
    DATE_DIFF('day',
        MIN(CAST(timestamp AS TIMESTAMP)),
        MAX(CAST(timestamp AS TIMESTAMP))
    ) as customer_lifetime_days
FROM bronze.transactions
GROUP BY account_id, customer_name
HAVING COUNT(*) > 5
ORDER BY lifetime_value DESC
LIMIT 20;

-- ============================================================================
-- 2. TREND ANALYSIS
-- ============================================================================

-- Daily trends with moving average
WITH daily_stats AS (
    SELECT
        CAST(timestamp AS DATE) as date,
        COUNT(*) as daily_transactions,
        ROUND(SUM(amount), 2) as daily_volume
    FROM bronze.transactions
    GROUP BY CAST(timestamp AS DATE)
)
SELECT
    date,
    daily_transactions,
    daily_volume,
    ROUND(AVG(daily_transactions) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_3day_txns,
    ROUND(AVG(daily_volume) OVER (
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_3day_volume
FROM daily_stats
ORDER BY date;

-- Week-over-week growth
WITH weekly_stats AS (
    SELECT
        YEAR(CAST(timestamp AS DATE)) as year,
        WEEK(CAST(timestamp AS DATE)) as week,
        COUNT(*) as transactions,
        ROUND(SUM(amount), 2) as volume
    FROM bronze.transactions
    GROUP BY
        YEAR(CAST(timestamp AS DATE)),
        WEEK(CAST(timestamp AS DATE))
)
SELECT
    year,
    week,
    transactions,
    volume,
    LAG(transactions) OVER (ORDER BY year, week) as prev_week_transactions,
    ROUND(
        100.0 * (transactions - LAG(transactions) OVER (ORDER BY year, week)) /
        NULLIF(LAG(transactions) OVER (ORDER BY year, week), 0),
        2
    ) as txn_growth_pct,
    ROUND(
        100.0 * (volume - LAG(volume) OVER (ORDER BY year, week)) /
        NULLIF(LAG(volume) OVER (ORDER BY year, week), 0),
        2
    ) as volume_growth_pct
FROM weekly_stats
ORDER BY year DESC, week DESC;

-- ============================================================================
-- 3. MERCHANT ANALYTICS
-- ============================================================================

-- Merchant performance matrix
SELECT
    merchant,
    COUNT(*) as transactions,
    COUNT(DISTINCT account_id) as unique_customers,
    ROUND(SUM(amount), 2) as total_revenue,
    ROUND(AVG(amount), 2) as avg_transaction,
    ROUND(SUM(amount) / COUNT(DISTINCT account_id), 2) as revenue_per_customer,
    ROUND(CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT account_id), 2) as transactions_per_customer,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate_pct
FROM bronze.transactions
WHERE transaction_type = 'PURCHASE'
GROUP BY merchant
HAVING COUNT(*) > 10
ORDER BY total_revenue DESC;

-- Merchant comparison (percentiles)
WITH merchant_stats AS (
    SELECT
        merchant,
        COUNT(*) as transactions,
        ROUND(SUM(amount), 2) as revenue
    FROM bronze.transactions
    WHERE transaction_type = 'PURCHASE'
    GROUP BY merchant
)
SELECT
    merchant,
    transactions,
    revenue,
    PERCENT_RANK() OVER (ORDER BY transactions) as transaction_percentile,
    PERCENT_RANK() OVER (ORDER BY revenue) as revenue_percentile
FROM merchant_stats
ORDER BY revenue DESC;

-- ============================================================================
-- 4. CUSTOMER SEGMENTATION
-- ============================================================================

-- RFM Analysis (Recency, Frequency, Monetary)
WITH customer_rfm AS (
    SELECT
        account_id,
        customer_name,
        DATE_DIFF('day', MAX(CAST(timestamp AS DATE)), CURRENT_DATE) as recency_days,
        COUNT(*) as frequency,
        ROUND(SUM(amount), 2) as monetary
    FROM bronze.transactions
    GROUP BY account_id, customer_name
)
SELECT
    account_id,
    customer_name,
    recency_days,
    frequency,
    monetary,
    CASE
        WHEN recency_days <= 7 AND frequency >= 10 AND monetary >= 5000 THEN 'VIP'
        WHEN recency_days <= 14 AND frequency >= 5 AND monetary >= 1000 THEN 'Active'
        WHEN recency_days <= 30 THEN 'Regular'
        ELSE 'Dormant'
    END as customer_segment
FROM customer_rfm
ORDER BY monetary DESC;

-- Customer spending distribution
SELECT
    CASE
        WHEN total_spend < 1000 THEN '< $1K'
        WHEN total_spend < 5000 THEN '$1K - $5K'
        WHEN total_spend < 10000 THEN '$5K - $10K'
        WHEN total_spend < 50000 THEN '$10K - $50K'
        ELSE '> $50K'
    END as spend_bucket,
    COUNT(*) as customer_count,
    ROUND(SUM(total_spend), 2) as bucket_total,
    ROUND(AVG(total_spend), 2) as bucket_avg
FROM (
    SELECT
        account_id,
        SUM(amount) as total_spend
    FROM bronze.transactions
    GROUP BY account_id
) customer_totals
GROUP BY
    CASE
        WHEN total_spend < 1000 THEN '< $1K'
        WHEN total_spend < 5000 THEN '$1K - $5K'
        WHEN total_spend < 10000 THEN '$5K - $10K'
        WHEN total_spend < 50000 THEN '$10K - $50K'
        ELSE '> $50K'
    END
ORDER BY
    CASE
        WHEN spend_bucket = '< $1K' THEN 1
        WHEN spend_bucket = '$1K - $5K' THEN 2
        WHEN spend_bucket = '$5K - $10K' THEN 3
        WHEN spend_bucket = '$10K - $50K' THEN 4
        ELSE 5
    END;

-- ============================================================================
-- 5. FRAUD ANALYSIS
-- ============================================================================

-- Fraud summary by merchant
SELECT
    merchant,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate_pct,
    ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) as fraud_amount,
    ROUND(SUM(amount), 2) as total_amount
FROM bronze.transactions
WHERE transaction_type = 'PURCHASE'
GROUP BY merchant
HAVING SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) > 0
ORDER BY fraud_rate_pct DESC;

-- High-risk accounts (multiple fraud flags)
SELECT
    account_id,
    customer_name,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_flags,
    ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate,
    ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) as suspected_fraud_amount,
    MAX(CAST(timestamp AS TIMESTAMP)) as last_transaction
FROM bronze.transactions
GROUP BY account_id, customer_name
HAVING SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) >= 2
ORDER BY fraud_flags DESC;

-- Fraud patterns by time
SELECT
    HOUR(CAST(timestamp AS TIMESTAMP)) as hour,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
    ROUND(100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) / COUNT(*), 2) as fraud_rate_pct
FROM bronze.transactions
GROUP BY HOUR(CAST(timestamp AS TIMESTAMP))
ORDER BY fraud_rate_pct DESC;

-- ============================================================================
-- 6. TRANSACTION PATTERNS
-- ============================================================================

-- Transaction type mix by merchant
SELECT
    merchant,
    COUNT(*) as total,
    SUM(CASE WHEN transaction_type = 'PURCHASE' THEN 1 ELSE 0 END) as purchases,
    SUM(CASE WHEN transaction_type = 'WITHDRAWAL' THEN 1 ELSE 0 END) as withdrawals,
    SUM(CASE WHEN transaction_type = 'DEPOSIT' THEN 1 ELSE 0 END) as deposits,
    SUM(CASE WHEN transaction_type = 'TRANSFER' THEN 1 ELSE 0 END) as transfers,
    SUM(CASE WHEN transaction_type = 'PAYMENT' THEN 1 ELSE 0 END) as payments
FROM bronze.transactions
GROUP BY merchant
HAVING COUNT(*) > 20
ORDER BY total DESC
LIMIT 15;

-- Multi-transaction accounts (potential velocity fraud)
WITH account_velocity AS (
    SELECT
        account_id,
        CAST(timestamp AS DATE) as date,
        COUNT(*) as txns_per_day,
        ROUND(SUM(amount), 2) as daily_volume
    FROM bronze.transactions
    GROUP BY account_id, CAST(timestamp AS DATE)
)
SELECT
    account_id,
    date,
    txns_per_day,
    daily_volume
FROM account_velocity
WHERE txns_per_day >= 10  -- Suspicious: 10+ transactions in one day
ORDER BY txns_per_day DESC, daily_volume DESC;

-- ============================================================================
-- 7. PERCENTILE ANALYSIS
-- ============================================================================

-- Transaction amount percentiles
SELECT
    ROUND(APPROX_PERCENTILE(amount, 0.25), 2) as p25,
    ROUND(APPROX_PERCENTILE(amount, 0.50), 2) as median,
    ROUND(APPROX_PERCENTILE(amount, 0.75), 2) as p75,
    ROUND(APPROX_PERCENTILE(amount, 0.90), 2) as p90,
    ROUND(APPROX_PERCENTILE(amount, 0.95), 2) as p95,
    ROUND(APPROX_PERCENTILE(amount, 0.99), 2) as p99
FROM bronze.transactions;

-- Transactions above 95th percentile (high-value)
WITH percentiles AS (
    SELECT APPROX_PERCENTILE(amount, 0.95) as p95
    FROM bronze.transactions
)
SELECT
    t.transaction_id,
    t.account_id,
    t.merchant,
    t.amount,
    t.transaction_type,
    CAST(t.timestamp AS TIMESTAMP) as time
FROM bronze.transactions t
CROSS JOIN percentiles p
WHERE t.amount >= p.p95
ORDER BY t.amount DESC;

-- ============================================================================
-- 8. FUNNEL ANALYSIS
-- ============================================================================

-- Transaction status funnel
SELECT
    status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(SUM(amount), 2) as total_amount
FROM bronze.transactions
GROUP BY status
ORDER BY
    CASE status
        WHEN 'COMPLETED' THEN 1
        WHEN 'PENDING' THEN 2
        WHEN 'FAILED' THEN 3
    END;

-- ============================================================================
-- Next: Try advanced_queries.sql for complex analytics!
-- ============================================================================
