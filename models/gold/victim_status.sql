WITH source AS (
    SELECT * FROM {{ ref('int_fraudDetail') }}
),

victim_trx_after_fraud AS (
    SELECT * FROM {{ ref('int_victimTrxAfterFraud') }}
),

customer_detail_source AS (
    SELECT * FROM {{ ref('int_customerDetail') }}
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
        customerCategory,
        recencyDays,
        frequency,
        monetaryValue
    FROM customer_detail_source
)

SELECT 
    fa.nameOrig as customerID,
    CASE 
        WHEN fv.customerID IS NOT NULL THEN 'Active'
        ELSE 'Inactive'
    END AS postFraudActivity,
    customerCategory,
    cd.recencyDays,
    cd.frequency,
    cd.monetaryValue
FROM fraudulent_accounts fa
LEFT JOIN frequent_victim fv ON fa.nameOrig = fv.customerID
LEFT JOIN customer_detail cd ON fa.nameOrig = cd.customerID