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
)

SELECT 
    fa.nameOrig as customerID,
    CASE 
        WHEN fv.customerID IS NOT NULL THEN 'Active'
        ELSE 'Inactive'
    END AS postFraudActivity
FROM fraudulent_accounts fa
LEFT JOIN frequent_victim fv ON fa.nameOrig = fv.customerID