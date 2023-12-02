{{
  config(
    materialized='table'
  )
}}

WITH FraudulentTransactions AS (
    SELECT DISTINCT nameDest
    FROM {{ source('final_project', 'fraud') }}
),

MultipleTransactions AS (
    SELECT DISTINCT nameDest
    FROM {{ ref('perpet_received_fraud') }}
)

SELECT 
    f.nameDest,
    CASE 
        WHEN f.nameDest IN (SELECT nameDest FROM MultipleTransactions) THEN 'ReceiveMultipleTrx'
        WHEN f.nameDest NOT IN (SELECT nameDest FROM MultipleTransactions) THEN 'ReceiveOneTrx'
    END AS TransactionCategory
FROM 
    FraudulentTransactions f