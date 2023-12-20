WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_fraud_confirmation`
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