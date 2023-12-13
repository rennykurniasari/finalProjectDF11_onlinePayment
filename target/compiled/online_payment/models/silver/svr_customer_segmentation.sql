WITH source AS (
  SELECT *
  FROM `final-project-404104`.`online_payment_transformed`.`svr_customer_detail`
),

percentiles AS (
    SELECT
        
    PERCENTILE_CONT(recencyDays, 0.2) OVER () AS recency_20,
    PERCENTILE_CONT(recencyDays, 0.4) OVER () AS recency_40,
    PERCENTILE_CONT(recencyDays, 0.6) OVER () AS recency_60,
    PERCENTILE_CONT(recencyDays, 0.8) OVER () AS recency_80,
    PERCENTILE_CONT(recencyDays, 1.0) OVER () AS recency_100,
    PERCENTILE_CONT(frequency, 0.2) OVER () AS frequency_20,
    PERCENTILE_CONT(frequency, 0.4) OVER () AS frequency_40,
    PERCENTILE_CONT(frequency, 0.6) OVER () AS frequency_60,
    PERCENTILE_CONT(frequency, 0.8) OVER () AS frequency_80,
    PERCENTILE_CONT(frequency, 1.0) OVER () AS frequency_100,
    PERCENTILE_CONT(monetaryValue, 0.2) OVER () AS monetary_20,
    PERCENTILE_CONT(monetaryValue, 0.4) OVER () AS monetary_40,
    PERCENTILE_CONT(monetaryValue, 0.6) OVER () AS monetary_60,
    PERCENTILE_CONT(monetaryValue, 0.8) OVER () AS monetary_80,
    PERCENTILE_CONT(monetaryValue, 1.0) OVER () AS monetary_100

    FROM source
    LIMIT 1
),

calc_rfm AS (
    SELECT
        customerID,
        
    CASE
        WHEN recencyDays < recency_20 THEN 5
        WHEN recencyDays >= recency_20 AND recencyDays < recency_40 THEN 4
        WHEN recencyDays >= recency_40 AND recencyDays < recency_60 THEN 3
        WHEN recencyDays >= recency_60 AND recencyDays < recency_80 THEN 2
        WHEN recencyDays >= recency_80 AND recencyDays <= recency_100 THEN 1
    END AS recencySegment,
    CASE
        WHEN frequency <= frequency_20 THEN 1
        WHEN frequency <= frequency_40 THEN 2
        WHEN frequency <= frequency_60 THEN 3
        WHEN frequency <= frequency_80 THEN 4
        WHEN frequency <= frequency_100 THEN 5
    END AS frequencySegment,
    CASE
        WHEN monetaryValue <= monetary_20 THEN 1
        WHEN monetaryValue <= monetary_40 THEN 2
        WHEN monetaryValue <= monetary_60 THEN 3
        WHEN monetaryValue <= monetary_80 THEN 4
        WHEN monetaryValue <= monetary_100 THEN 5
    END AS monetarySegment

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
        
    WHEN rfmScore in (555, 554, 544, 545, 454, 455, 445) THEN 'Champion'
    WHEN rfmScore in (543, 444, 435, 355, 354, 345, 344, 335) THEN 'Loyal Customers'
    WHEN rfmScore in (553, 551, 552, 541, 542, 533, 532, 531, 452, 451, 442, 441, 431, 453, 433, 432, 423, 353, 352, 351, 342, 341, 333, 323) THEN 'Potential Loyalist'
    WHEN rfmScore in (512, 511, 422, 421, 412, 411, 311) THEN 'Recent Customer'
    WHEN rfmScore in (525, 524, 523, 522, 521, 515, 514, 513, 425, 424, 413, 414, 415, 315, 314, 313) THEN 'Promising'
    WHEN rfmScore in (535, 534, 443, 434, 343, 334, 325, 324) THEN 'Need Attention'
    WHEN rfmScore in (331, 321, 312, 221, 213) THEN 'About To Sleep'
    WHEN rfmScore in (255, 254, 245, 244, 253, 252, 243, 242, 235, 234, 225, 224, 153, 152, 145, 143, 142, 135, 134, 133, 125, 124) THEN 'At Risk'
    WHEN rfmScore in (155, 154, 144, 214, 215, 115, 114, 113) THEN 'Can’t Lose Them'
    WHEN rfmScore in (332, 322, 231, 241, 251, 233, 232, 223, 222, 132, 123, 122, 212, 211) THEN 'Hibernating'
    WHEN rfmScore in (111, 112, 121, 131, 141, 151) THEN 'Lost'
    ELSE 'Other'

    END AS customerType
FROM rfm_segments
ORDER BY rfmScore DESC