WITH customer_detail AS (
  SELECT * FROM {{ ref('int_customerDetail') }}
),

customer_segmentation AS (
  SELECT * FROM {{ ref('int_customerSegmentation') }}
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