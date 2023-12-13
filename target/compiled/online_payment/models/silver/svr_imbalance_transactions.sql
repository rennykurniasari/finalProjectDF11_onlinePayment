WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`
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
            
    (ABS(oldBalanceOrig - amount - newBalanceOrig) <= 0.01 OR
    ROUND(oldBalanceOrig - amount, 2) = ROUND(newBalanceOrig, 2)) AND
    (ABS(oldBalanceDest + amount - newBalanceDest) <= 0.01 OR
    ROUND(oldBalanceDest + amount, 2) = ROUND(newBalanceDest, 2))
 THEN True

            WHEN type = 'CASH_IN' AND
            
    (ABS(oldBalanceOrig + amount - newBalanceOrig) <= 0.01 OR
    ROUND(oldBalanceOrig + amount, 2) = ROUND(newBalanceOrig, 2)) AND
    (ABS(oldBalanceDest - amount - newBalanceDest) <= 0.01 OR
    ROUND(oldBalanceDest - amount, 2) = ROUND(newBalanceDest, 2))
 THEN True

            ELSE False

        END AS isValid,
        isFraud,
        isFlaggedFraud
    FROM source
)

SELECT * FROM imbalance_transactions
ORDER BY transactionID