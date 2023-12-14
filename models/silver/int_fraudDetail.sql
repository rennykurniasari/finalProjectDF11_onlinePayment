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
    t.amount,
    s.isFraud,
    s.isFlaggedFraud,
    s.isConfirmed,
    s.isTrueFraud,
    t.nameOrig,
    t.oldBalanceOrig,
    t.newBalanceOrig,
    t.nameDest,
    t.oldBalanceDest,
    t.newBalanceDest
FROM
    source s
LEFT JOIN transactions t ON s.transactionID = t.transactionID
ORDER BY transactionID