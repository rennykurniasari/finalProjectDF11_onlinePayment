{{
  config(
    materialized='table'
  )
}}

WITH FirstFraudInstance AS (
    SELECT
        nameOrig,
        MIN(payment_datetime) AS FirstFraudDate
    FROM
        {{ ref('fill_transactionValid') }}
    WHERE
        isFraud = 1
    GROUP BY
        nameOrig
)

SELECT
    t.nameOrig,
    t.payment_datetime,
    t.isFraud
FROM
    {{ ref('fill_transactionValid') }} t
INNER JOIN
    FirstFraudInstance f ON t.nameOrig = f.nameOrig
WHERE
    t.payment_datetime < f.FirstFraudDate
ORDER BY
    t.nameOrig,
    t.payment_datetime
