{{
  config(
    materialized='table'
  )
}}

SELECT idTransaction, isFraud, isTrueFraud, refund, confirm, newBalanceOriginSet, oldBalanceOrigin, timestamp, amount
FROM {{ source('final_project', 'fraud') }}
WHERE isFraud=1 AND isTrueFraud=1