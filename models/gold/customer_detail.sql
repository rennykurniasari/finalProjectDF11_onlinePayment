WITH customer_detail AS (
  SELECT * FROM {{ ref('svr_customer_detail') }}
),

customer_segmentation AS (
  SELECT * FROM {{ ref('svr_customer_segmentation') }}
)

SELECT
    cd.customerID,
    cd.customerCategory,
    cs.customerType,
    cd.customerStatus,
    cd.recencyDays,
    cd.frequency,
    cd.monetaryValue
FROM customer_detail cd
LEFT JOIN customer_segmentation cs ON cd.customerID = cs.customerID
ORDER BY customerID