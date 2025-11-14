SELECT
    transaction_date::date AS date_key,
    merchant_id AS merchant_key,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    SUM(CASE WHEN auth_result = 'DECLINED' THEN 1 ELSE 0 END) AS declined_count
FROM gold.fact_transactions_raw
GROUP BY 1, 2;
