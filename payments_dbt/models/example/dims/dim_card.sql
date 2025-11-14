SELECT DISTINCT card_hash AS card_key,
    card_hash,
    channel
FROM { { ref('fact_transactions') } };