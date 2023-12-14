WITH source AS (

    SELECT * FROM {{ source('online_payment', 'fraud_confirmation') }}

)

SELECT
    idTransaction as transactionID,
    CAST(isFraud as BOOLEAN) as isFraud,
    CAST(isFlaggedFraud as BOOLEAN) as isFlaggedFraud,
    CAST(confirm as BOOLEAN) as isConfirmed,
    CAST(isTrueFraud as BOOLEAN) as isTrueFraud,
    CAST(refund as BOOLEAN) as isRefunded,
    ROUND(oldBalanceOriginSet, 2) as oldBalanceOrigSet,
    ROUND(newBalanceOriginSet, 2) as newBalanceOrigSet,
    ROUND(oldBalanceDestSet, 2) as oldBalanceDestSet,
    ROUND(newBalanceDestSet, 2) as newBalanceDestSet
FROM
    source
ORDER BY transactionID