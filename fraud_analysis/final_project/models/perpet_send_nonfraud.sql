{{
  config(
    materialized='table'
  )
}}

WITH FraudulentSenders AS (
    SELECT DISTINCT nameDest
    FROM {{ ref('fill_transactionValid') }}
    WHERE isFraud = 1
)

SELECT 
    a.payment_id, 
    a.payment_datetime, 
    a.nameOrig, 
    a.nameDest
FROM 
    {{ ref('fill_transactionValid') }} a
JOIN 
    FraudulentSenders fs ON a.nameOrig = fs.nameDest
WHERE 
    a.isFraud = 0
