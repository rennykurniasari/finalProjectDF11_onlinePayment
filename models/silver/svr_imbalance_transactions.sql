WITH source AS (
    SELECT * FROM {{ ref('brz_transactions') }}
),

imbalance_transactions AS (
    SELECT
        transactionID,
        transactionDatetime,
        type,
        ROUND(amount, 2) as amount,
        nameOrig,
        ROUND(oldBalanceOrig, 2) as oldBalanceOrig,
        ROUND(newbalanceOrig, 2) as newbalanceOrig,
        nameDest,
        ROUND(oldbalanceDest, 2) as oldbalanceDest,
        ROUND(newbalanceDest, 2) as newbalanceDest,
        CASE
            WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
            {{ trx_validation_decreaseNewOrig() }} THEN True

            WHEN type = 'CASH_IN' AND
            {{ trx_validation_increaseNewOrig() }} THEN True

            ELSE False

        END AS isValid,
        isFraud,
        isFlaggedFraud
    FROM source
)

SELECT * FROM imbalance_transactions
ORDER BY transactionID