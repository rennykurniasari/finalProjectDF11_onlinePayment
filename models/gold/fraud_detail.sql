WITH source AS (
  SELECT * FROM {{ ref('svr_fraud_detail') }}
)

SELECT
    transactionID,
    transactionDatetime,
    type,
    amount,
    CASE
        WHEN amount >= 0 AND amount <= 1000000 THEN '0 - 1M'
        WHEN amount > 1000000 AND amount <= 2000000 THEN '1M - 2M'
        WHEN amount > 2000000 AND amount <= 3000000 THEN '2M - 3M'
        WHEN amount > 3000000 AND amount <= 4000000 THEN '3M - 4M'
        WHEN amount > 4000000 AND amount <= 5000000 THEN '4M - 5M'
        WHEN amount > 5000000 AND amount <= 6000000 THEN '5M - 6M'
        WHEN amount > 6000000 AND amount <= 7000000 THEN '6M - 7M'
        WHEN amount > 7000000 AND amount <= 8000000 THEN '7M - 8M'
        WHEN amount > 8000000 AND amount <= 9000000 THEN '8M - 9M'
        WHEN amount > 9000000 AND amount <= 10000000 THEN '9M - 10M'
    END AS valueRange,
    isFraud,
    isFlaggedFraud,
    isConfirmed,
    isTrueFraud,
    nameOrig,
    oldBalanceOrig,
    newBalanceOrig,
    nameDest,
    oldBalanceDest,
    newBalanceDest
FROM source