{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT payment_id, nameDest, payment_datetime, isFraud
FROM 
  ( SELECT *, 
           count(1) OVER (PARTITION BY nameDest) AS occurs
    FROM {{ ref('fill_transactionValid') }}
    WHERE isFraud=1
  ) AS t 
WHERE occurs > 1
ORDER BY nameDest, payment_datetime