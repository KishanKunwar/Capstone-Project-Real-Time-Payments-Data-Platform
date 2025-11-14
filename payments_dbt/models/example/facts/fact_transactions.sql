SELECT
    transaction_id,
    transaction_date,
    card_hash AS card_key,
    merchant_id AS merchant_key,
    amount,
    currency,
    auth_result
FROM gold.fact_transactions_raw;
