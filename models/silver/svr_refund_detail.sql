WITH source AS (
    SELECT * FROM {{ ref('brz_fraud_confirmation') }}
)

SELECT
    transactionID,
    transactionDatetime,
    type,
    isRefunded,
    amount as refundedAmount,
    oldBalanceOrigSet,
    newBalanceOrigSet,
    oldBalanceDestSet,
    newBalanceDestSet
FROM
    source
ORDER BY transactionID