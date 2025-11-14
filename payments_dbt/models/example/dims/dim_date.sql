SELECT DISTINCT transaction_date AS date_key,
    EXTRACT(
        YEAR
        FROM transaction_date
    ) AS year,
    EXTRACT(
        MONTH
        FROM transaction_date
    ) AS month,
    EXTRACT(
        DAY
        FROM transaction_date
    ) AS day
FROM { { ref('fact_transactions') } };