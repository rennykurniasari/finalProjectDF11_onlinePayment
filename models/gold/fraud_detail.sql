WITH source AS (
  SELECT * FROM {{ ref('int_fraudDetail') }}
),

transaction_validation AS (
  SELECT * FROM {{ ref('int_balancingTransactions') }}
),

refund_validation AS (
  SELECT * FROM {{ ref('int_refundDetail') }}
)

SELECT
    s.transactionID,
    s.transactionDatetime,
    s.type,
    s.amount,
    CASE
        WHEN s.amount >= 0 AND s.amount <= 1000000 THEN '0 - 1M'
        WHEN s.amount > 1000000 AND s.amount <= 2000000 THEN '1M - 2M'
        WHEN s.amount > 2000000 AND s.amount <= 3000000 THEN '2M - 3M'
        WHEN s.amount > 3000000 AND s.amount <= 4000000 THEN '3M - 4M'
        WHEN s.amount > 4000000 AND s.amount <= 5000000 THEN '4M - 5M'
        WHEN s.amount > 5000000 AND s.amount <= 6000000 THEN '5M - 6M'
        WHEN s.amount > 6000000 AND s.amount <= 7000000 THEN '6M - 7M'
        WHEN s.amount > 7000000 AND s.amount <= 8000000 THEN '7M - 8M'
        WHEN s.amount > 8000000 AND s.amount <= 9000000 THEN '8M - 9M'
        WHEN s.amount > 9000000 AND s.amount <= 10000000 THEN '9M - 10M'
    END AS valueRange,
    s.isFraud,
    s.isFlaggedFraud,
    s.isConfirmed,
    s.isTrueFraud,
    rv.isRefunded,
    s.nameOrig,
    s.oldBalanceOrig,
    s.newBalanceOrig,
    s.nameDest,
    s.oldBalanceDest,
    s.newBalanceDest,
    tv.isValid
FROM source s
LEFT JOIN transaction_validation tv ON s.transactionID = tv.transactionID
LEFT JOIN refund_validation rv ON s.transactionID = rv.transactionID