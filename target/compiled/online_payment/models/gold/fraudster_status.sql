WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`
),

fraud_detail AS (
    SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_fraud_detail`
),

frequent_fraudsters AS (
    SELECT DISTINCT
        nameDest AS customerID,
        occurs AS fraudOccurs
    FROM (
        SELECT
            *,
            COUNT(transactionID) OVER (PARTITION BY nameDest) AS occurs
        FROM source
        WHERE isFraud = True
    ) AS t
    WHERE occurs > 1
),

fraudulent_transactions AS (
    SELECT DISTINCT
        nameDest
    FROM fraud_detail
),

customer_detail AS (
    SELECT
        DISTINCT customerID,
        customerCategory,
        recencyDays,
        frequency,
        monetaryValue
    FROM `final-project-404104`.`online_payment_transformed`.`svr_customer_detail`
)

SELECT 
    ft.nameDest AS customerID,
    CASE 
        WHEN ft.nameDest IN (SELECT customerID FROM frequent_fraudsters) THEN 'Frequent Fraudster'
        ELSE 'Infrequent Fraudster'
    END AS fraudsterCategory,
    cd.customerCategory,
    cd.recencyDays,
    cd.frequency,
    cd.monetaryValue
FROM fraudulent_transactions ft
LEFT JOIN customer_detail cd ON ft.nameDest = cd.customerID