WITH source AS (
    SELECT * FROM {{ ref('brz_fraud_confirmation') }}
)

SELECT
    transactionID,
    transactionDatetime,
    type,
    amount,
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
FROM
    source
ORDER BY transactionID