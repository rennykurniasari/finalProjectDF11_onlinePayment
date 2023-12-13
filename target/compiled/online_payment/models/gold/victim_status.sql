WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_fraud_confirmation`
),

victim_trx_after_fraud AS (
    SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_victim_trx_after_fraud`
),

fraudulent_accounts AS (
    SELECT
        DISTINCT nameOrig
    FROM source
),

frequent_victim AS (
    SELECT
        DISTINCT customerID
    FROM victim_trx_after_fraud
),

customer_detail AS (
    SELECT
        DISTINCT customerID,
        recencyDays,
        frequency,
        monetaryValue
    FROM `final-project-404104`.`online_payment_transformed`.`svr_customer_detail`
)

SELECT 
    fa.nameOrig as customerID,
    CASE 
        WHEN fv.customerID IS NOT NULL THEN 'Active'
        ELSE 'Inactive'
    END AS postFraudActivity,
    cd.recencyDays,
    cd.frequency,
    cd.monetaryValue
FROM fraudulent_accounts fa
LEFT JOIN frequent_victim fv ON fa.nameOrig = fv.customerID
LEFT JOIN customer_detail cd ON fa.nameOrig = cd.customerID