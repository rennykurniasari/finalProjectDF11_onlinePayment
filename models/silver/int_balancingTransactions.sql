WITH source AS (
  SELECT *
  FROM {{ ref('int_imbalanceTransactions') }}
),

balancing_transaction AS (
    SELECT *,
        CASE
            WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
            isValid = False THEN ROUND(oldBalanceOrig - amount, 2)

            WHEN type = 'CASH_IN' AND
            isValid = False THEN ROUND(oldBalanceOrig + amount, 2)

            ELSE newBalanceOrig

        END AS newBalanceOrigValid,

        CASE
            WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
            isValid = False THEN ROUND(oldBalanceDest + amount, 2)

            WHEN type = 'CASH_IN' AND
            isValid = False THEN ROUND(oldBalanceDest - amount, 2)

            ELSE newBalanceDest

        END AS newBalanceDestValid
    FROM source
),

balancing_validation AS (
    SELECT
        transactionID,
        CASE
            WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
            {{ trx_validation_decreaseNewOrigValid() }} THEN True

            WHEN type = 'CASH_IN' AND
            {{ trx_validation_increaseNewOrigValid() }} THEN True

            ELSE False

        END AS isValidUpdate
    FROM balancing_transaction
)

SELECT
    bt.transactionID as transactionID,
    bt.transactionDatetime,
    bt.type,
    bt.amount,
    bt.nameOrig,
    bt.oldBalanceOrig,
    bt.newbalanceOrig,
    bt.newBalanceOrigValid,
    bt.nameDest,
    bt.oldbalanceDest,
    bt.newbalanceDest,
    bt.newBalanceDestValid,
    bt.isValid,
    bv.isValidUpdate,
    bt.isFraud,
    bt.isFlaggedFraud
FROM balancing_transaction AS bt
LEFT JOIN balancing_validation AS bv ON bt.transactionID = bv.transactionID
ORDER BY transactionID
