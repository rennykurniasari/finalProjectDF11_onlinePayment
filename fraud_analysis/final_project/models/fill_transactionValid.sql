{{
  config(
    materialized='table'
  )
}}

WITH base_data AS (
    SELECT *,
           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
               {{ trx_validation_decreaseNewOrig() }} THEN 'True'
               WHEN type = 'CASH_IN' AND
               {{ trx_validation_increaseNewOrig() }} THEN 'True'
               ELSE 'False'

           END AS isValid,

           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND {{ trx_validation_decreaseNewOrig() }}
               THEN ROUND(oldbalanceOrg - amount, 1)
               WHEN type = 'CASH_IN' AND {{ trx_validation_increaseNewOrig() }}
               THEN ROUND(oldbalanceOrg + amount, 1)
               ELSE newbalanceOrig

           END AS newbalanceOrig_valid,

           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND {{ trx_validation_decreaseNewOrig() }}
               THEN ROUND(oldbalanceDest + amount, 1)
               WHEN type = 'CASH_IN' AND {{ trx_validation_increaseNewOrig() }}
               THEN ROUND(oldbalanceDest - amount, 1)            
               ELSE newbalanceDest

           END AS newbalanceDest_valid

    FROM {{ source('final_project', 'all_data') }}
)

SELECT *
FROM base_data