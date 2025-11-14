SELECT
    transaction_id,
    amount,
    (amount > 900)::boolean AS high_amount_flag,
    (currency NOT IN ('USD','CAD','EUR')) AS invalid_currency_flag
FROM gold.fact_transactions_raw;
