WITH source AS (
    SELECT * FROM {{ ref('stg_fraudConfirmation') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions')}}
)

SELECT
    s.transactionID,
    t.transactionDatetime,
    t.type,
    s.isRefunded,
    t.amount as refundedAmount,
    s.oldBalanceOrigSet,
    s.newBalanceOrigSet,
    s.oldBalanceDestSet,
    s.newBalanceDestSet
FROM
    source s
LEFT JOIN transactions t ON s.transactionID = t.transactionID
ORDER BY transactionID