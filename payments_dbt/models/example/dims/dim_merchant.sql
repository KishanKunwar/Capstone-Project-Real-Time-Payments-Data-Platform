SELECT DISTINCT merchant_id AS merchant_key,
    merchant_id,
    mcc
FROM { { ref('fact_transactions') } };