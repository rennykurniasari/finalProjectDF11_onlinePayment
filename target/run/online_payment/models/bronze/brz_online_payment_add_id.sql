

  create or replace view `final-project-404104`.`online_payment_raw`.`brz_online_payment_add_id`
  OPTIONS()
  as WITH NumberedRows AS (

    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY payment_datetime) AS row_num
    FROM `final-project-404104`.`online_payment_raw`.`brz_online_payment`

)

SELECT
    CONCAT('P', LPAD(CAST(row_num AS STRING), 10, '0'), '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', payment_datetime)) AS payment_id,
    payment_datetime,
    type,
    amount,
    nameOrig,
    oldbalanceOrg,
    newbalanceOrig,
    nameDest,
    oldbalanceDest,
    newbalanceDest,
    isFraud,
    isFlaggedFraud,
    step
FROM
  NumberedRows
ORDER BY payment_id;

