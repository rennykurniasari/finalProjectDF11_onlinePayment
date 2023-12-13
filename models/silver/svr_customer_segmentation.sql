WITH source AS (
  SELECT *
  FROM {{ ref('svr_customer_detail') }}
),

percentiles AS (
    SELECT
        {{ percentiles_count() }}
    FROM source
    LIMIT 1
),

calc_rfm AS (
    SELECT
        customerID,
        {{ rfm_calculation() }}
    FROM source
    CROSS JOIN percentiles
),

rfm_segments AS (
    SELECT
        calc_rfm.*,
        CAST(CONCAT(
          recencySegment,
          frequencySegment,
          monetarySegment
        ) as INTEGER) AS rfmScore
    FROM calc_rfm
)

SELECT
    customerID,
    recencySegment,
    frequencySegment,
    monetarySegment,
    rfmScore,
    CASE
        {{ rfm_segmentation1() }}
    END AS customerType
FROM rfm_segments
ORDER BY rfmScore DESC