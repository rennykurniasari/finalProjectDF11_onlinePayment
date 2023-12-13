
  
    

    create or replace table `final-project-404104`.`online_payment_transformed`.`customer_segmentation`
      
    
    

    OPTIONS()
    as (
      WITH source AS (
  SELECT *
  FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`
),

rfm_raw AS (
  SELECT
    nameOrig,
    DATE_DIFF(DATE(CURRENT_DATETIME()), DATE(MAX(transactionDatetime)), DAY) AS recency_days,
    COUNT(*) AS frequency,
    SUM(amount) AS monetary_value
  FROM source
  GROUP BY nameOrig
),

calc_rfm AS (
  SELECT
    rfm_raw.*,
    NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_segment,
    NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_segment,
    NTILE(5) OVER (ORDER BY monetary_value DESC) AS monetary_value_segment
  FROM rfm_raw
),

rfm_segments AS (
  SELECT
    calc_rfm.*,
    CAST(CONCAT(
      recency_segment,
      frequency_segment,
      monetary_value_segment
    ) as INTEGER) AS rfm_score
  FROM calc_rfm
)

SELECT
  nameOrig,
  recency_segment,
  frequency_segment,
  monetary_value_segment,
  rfm_score,
  CASE
    
    WHEN recency_segment >= 4 AND recency_segment <= 5 AND frequency_segment >= 4 AND frequency_segment <= 5 AND monetary_value_segment >= 4 AND monetary_value_segment <= 5 THEN 'Champions'
    WHEN recency_segment >= 2 AND recency_segment <= 5 AND frequency_segment >= 3 AND frequency_segment <= 5 AND monetary_value_segment >= 3 AND monetary_value_segment <= 5
        OR rfm_score IN (524, 525, 542)
    THEN 'Loyal Customers'
    WHEN recency_segment >= 3 AND recency_segment <= 5 AND frequency_segment >= 1 AND frequency_segment <= 3 AND monetary_value_segment >= 1 AND monetary_value_segment <= 3
        OR rfm_score IN (341, 342, 351, 352)
    THEN 'Potential Loyalist'
    WHEN recency_segment >= 4 AND recency_segment <= 5 AND frequency_segment = 1 AND monetary_value_segment = 1
        OR rfm_score IN (514, 515, 541, 551, 552)
    THEN 'Recent Customers'
    WHEN recency_segment >= 3 AND recency_segment <= 4 AND frequency_segment = 1 AND monetary_value_segment = 1
        OR rfm_score IN (414, 415, 424, 425, 441, 442, 451, 452)
    THEN 'Promising'
    WHEN recency_segment >= 2 AND recency_segment <= 3 AND frequency_segment >= 2 AND frequency_segment <= 3 AND monetary_value_segment >= 2 AND monetary_value_segment <= 3
        OR rfm_score IN (314, 315)
    THEN 'Need Attention'
    WHEN recency_segment >= 2 AND recency_segment <= 3 AND frequency_segment >= 1 AND frequency_segment <= 2 AND monetary_value_segment >= 1 AND monetary_value_segment <= 2
        OR rfm_score IN (324, 325)
    THEN 'About To Sleep'
    WHEN recency_segment >= 1 AND recency_segment <= 2 AND frequency_segment >= 2 AND frequency_segment <= 5 AND monetary_value_segment >= 2 AND monetary_value_segment <= 5 THEN 'At Risk'
    WHEN recency_segment = 1 AND frequency_segment >= 4 AND frequency_segment <= 5 AND monetary_value_segment >= 4 AND monetary_value_segment <= 5
        OR rfm_score IN (113, 114, 115, 213, 214, 215)
    THEN 'Can’t Lose Them'
    WHEN recency_segment >= 1 AND recency_segment <= 2 AND frequency_segment >= 1 AND frequency_segment <= 2 AND monetary_value_segment >= 2 AND monetary_value_segment <= 3
        OR rfm_score IN (131, 141, 151, 231, 241, 251)
    THEN 'Hibernating'
    WHEN recency_segment = 1 AND frequency_segment = 1 AND monetary_value_segment = 1 THEN 'Lost'
    ELSE 'Other'

  END AS customer_type
FROM rfm_segments
ORDER BY rfm_score DESC
    );
  