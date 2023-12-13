WITH source AS (
    SELECT * FROM {{ source('brz_online_payment', 'brz_fraud_confirmation') }}
)

SELECT
    idTransaction as transactionID,
    timestamp as transactionDatetime,
    type,
    ROUND(amount, 2) as amount,
    CAST(isFraud as BOOLEAN) as isFraud,
    CAST(isFlaggedFraud as BOOLEAN) as isFlaggedFraud,
    CAST(confirm as BOOLEAN) as isConfirmed,
    CAST(isTrueFraud as BOOLEAN) as isTrueFraud,
    nameOrigin as nameOrig,
    ROUND(oldBalanceOrigin, 2) as oldBalanceOrig,
    ROUND(newBalanceOrigin, 2) as newBalanceOrig,
    nameDest,
    ROUND(oldbalanceDest, 2) as oldBalanceDest,
    ROUND(newbalanceDest, 2) as newBalanceDest,
    CAST(refund as BOOLEAN) as isRefunded,
    ROUND(oldBalanceOriginSet, 2) as oldBalanceOrigSet,
    ROUND(newBalanceOriginSet, 2) as newBalanceOrigSet,
    ROUND(oldBalanceDestSet, 2) as oldBalanceDestSet,
    ROUND(newBalanceDestSet, 2) as newBalanceDestSet
FROM
    source
ORDER BY transactionID