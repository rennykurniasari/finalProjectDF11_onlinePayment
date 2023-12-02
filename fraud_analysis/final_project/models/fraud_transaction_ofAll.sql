{{
  config(
    materialized='table'
  )
}}

SELECT payment_id, isFraud, payment_datetime, amount, isFlaggedFraud
FROM {{ ref('fill_transactionValid') }}