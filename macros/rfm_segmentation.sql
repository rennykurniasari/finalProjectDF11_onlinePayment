{% macro percentiles_count() %}
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
{% endmacro %}

{% macro rfm_calculation() %}
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
{% endmacro %}

{% macro rfm_segmentation() %}
    WHEN recencySegment >= 4 AND recencySegment <= 5 AND frequencySegment >= 4 AND frequencySegment <= 5 AND monetarySegment >= 4 AND monetarySegment <= 5 THEN 'Champions'
    WHEN recencySegment >= 2 AND recencySegment <= 5 AND frequencySegment >= 3 AND frequencySegment <= 5 AND monetarySegment >= 3 AND monetarySegment <= 5
        OR rfmScore IN (524, 525, 542)
    THEN 'Loyal Customers'
    WHEN recencySegment >= 4 AND recencySegment <= 5 AND frequencySegment = 1 AND monetarySegment = 1
        OR rfmScore IN (514, 515, 541, 551, 552)
    THEN 'Recent Customers'
    WHEN recencySegment >= 3 AND recencySegment <= 5 AND frequencySegment >= 1 AND frequencySegment <= 3 AND monetarySegment >= 1 AND monetarySegment <= 3
        OR rfmScore IN (341, 342, 351, 352)
    THEN 'Potential Loyalist'
    WHEN recencySegment >= 3 AND recencySegment <= 4 AND frequencySegment = 1 AND monetarySegment = 1
        OR rfmScore IN (414, 415, 424, 425, 441, 442, 451, 452)
    THEN 'Promising'
    WHEN recencySegment >= 2 AND recencySegment <= 3 AND frequencySegment >= 2 AND frequencySegment <= 3 AND monetarySegment >= 2 AND monetarySegment <= 3
        OR rfmScore IN (314, 315)
    THEN 'Need Attention'
    WHEN recencySegment >= 2 AND recencySegment <= 3 AND frequencySegment >= 1 AND frequencySegment <= 2 AND monetarySegment >= 1 AND monetarySegment <= 2
        OR rfmScore IN (324, 325)
    THEN 'About To Sleep'
    WHEN recencySegment >= 1 AND recencySegment <= 2 AND frequencySegment >= 2 AND frequencySegment <= 5 AND monetarySegment >= 2 AND monetarySegment <= 5 THEN 'At Risk'
    WHEN recencySegment <= 3 AND frequencySegment >= 4 AND frequencySegment <= 5 AND monetarySegment >= 4 AND monetarySegment <= 5
        OR rfmScore IN (113, 114, 115, 213, 214, 215)
    THEN 'Can’t Lose Them'
    WHEN recencySegment >= 1 AND recencySegment <= 2 AND frequencySegment >= 1 AND frequencySegment <= 2 AND monetarySegment >= 2 AND monetarySegment <= 3
        OR rfmScore IN (121, 131, 141, 151, 231, 241, 251)
    THEN 'Hibernating'
    WHEN recencySegment = 1 AND frequencySegment = 1 AND monetarySegment = 1 THEN 'Lost'
    ELSE 'Other'
{% endmacro %}