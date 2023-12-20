

  create or replace view `final-project-404104`.`online_payment_raw`.`brz_transactions`
  OPTIONS()
  as WITH NumberedRows AS (

    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY payment_datetime) AS row_num
    FROM `final-project-404104`.`online_payment_raw`.`brz_transactions_merge`

)

SELECT
    CONCAT('P', LPAD(CAST(row_num AS STRING), 10, '0'), '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', payment_datetime)) AS transactionID,
    payment_datetime as transactionDatetime,
    type,
    ROUND(amount, 2) as amount,
    nameOrig,
    ROUND(oldbalanceOrg, 2) as oldBalanceOrig,
    ROUND(newbalanceOrig, 2) as newBalanceOrig,
    nameDest,
    ROUND(oldbalanceDest, 2) as oldBalanceDest,
    ROUND(newbalanceDest, 2) as newBalanceDest,
    CAST(isFraud as BOOLEAN) as isFraud,
    CAST(isFlaggedFraud as BOOLEAN) as isFlaggedFraud,
    step
FROM
    NumberedRows
ORDER BY transactionID;

