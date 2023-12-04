{{
  config(
    materialized='table'
  )
}}

WITH FraudulentAccounts AS (
    SELECT DISTINCT nameOrigin
    FROM {{ source('final_project', 'fraud') }}
),

RepeatVictimFraud AS (
    SELECT DISTINCT nameOrig
    FROM {{ ref('trx_victim_after_fraud') }}
)

SELECT 
    fa.nameOrigin,
    CASE 
        WHEN rc.nameOrig IS NOT NULL THEN 'Repeat Transaction'
        ELSE 'Inactive Post-Fraud'
    END AS AccountActivityStatus
FROM FraudulentAccounts fa
LEFT JOIN RepeatVictimFraud rc ON fa.nameOrigin = rc.nameOrig