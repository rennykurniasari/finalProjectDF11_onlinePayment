WITH source AS (
    SELECT * FROM {{ ref('brz_transactions') }}
),

od_customer_trx AS (
    SELECT
        d.nameDest as nameDest_d,
        o.nameOrig as nameOrig_o,
    FROM source d
    LEFT JOIN source o ON d.nameDest = o.nameOrig
    WHERE d.transactionID <> o.transactionID
),

od_customer AS (
    SELECT
        DISTINCT nameOrig_o as nameDest,
        CASE WHEN nameOrig_o = nameDest_d THEN True ELSE False END AS isOrig,
        CASE WHEN nameOrig_o = nameDest_d THEN True ELSE False END AS isDest
    FROM od_customer_trx
),

o_customer AS (
    SELECT
        DISTINCT nameOrig,
        CASE WHEN nameOrig IS NOT NULL THEN True ELSE False END AS isOrig,
        CASE WHEN nameOrig IS NOT NULL THEN False ELSE True END AS isDest
    FROM source
    EXCEPT DISTINCT
    SELECT
        nameDest, isOrig, isDest
    FROM od_customer
),

d_customer AS (
    SELECT
        DISTINCT nameDest,
        CASE WHEN nameDest IS NOT NULL THEN False ELSE True END AS isOrig,
        CASE WHEN nameDest IS NOT NULL THEN True ELSE False END AS isDest
    FROM source
    EXCEPT DISTINCT
    SELECT
        nameDest, isOrig, isDest
    FROM od_customer
),

merge_customer AS (
    SELECT * FROM od_customer
    UNION ALL
    SELECT * FROM o_customer
    UNION ALL
    SELECT * FROM d_customer
),

unique_customers AS (
    SELECT
        nameDest as customerID,
        isOrig,
        isDest
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY nameDest ORDER BY isOrig DESC) AS row_num
        FROM merge_customer
    )
    WHERE row_num = 1
),

customer_rfm AS (
  SELECT
    nameOrig,
    DATE_DIFF(DATE(CURRENT_DATETIME()), DATE(MAX(transactionDatetime)), DAY) AS recencyDays,
    COUNT(*) AS frequency,
    ROUND(SUM(amount), 2) AS monetaryValue
  FROM source
  GROUP BY nameOrig
),

fraudulent_activity AS (
    SELECT
        DISTINCT nameDest AS customerID
    FROM source
    WHERE isFraud = True
),

suspect_activity AS (
    SELECT
        DISTINCT nameOrig AS customerID
    FROM source
    WHERE nameDest IN (SELECT customerID FROM {{ ref('svr_fraudster_receive_nonfraud_trx') }}) AND isFraud = False
    UNION DISTINCT
    SELECT
        DISTINCT customerID
    FROM {{ ref('svr_fraudster_send_nonfraud_trx') }}
),

victim_activity AS (
    SELECT
        DISTINCT nameOrig AS customerID
    FROM source
    WHERE isFraud = True
),

customer_detail AS (
    SELECT
        uc.customerID,
        uc.isOrig,
        uc.isDest,
        CASE 
            WHEN uc.isOrig = True AND uc.isDest=False THEN 'Sender'
            WHEN uc.isOrig = False AND uc.isDest=True THEN 'Receiver'
            WHEN uc.isOrig = True AND uc.isDest = True THEN 'Transactor'
        END AS customerCategory,
        CASE
            WHEN (uc.isOrig = True AND uc.isDest = True)
            OR (uc.isOrig = True AND uc.isDest = False) THEN cr.recencyDays
            ELSE DATE_DIFF(DATE(CURRENT_DATETIME()), DATE('2022-12-31'), DAY)
        END AS recencyDays,
        CASE
            WHEN (uc.isOrig = True AND uc.isDest = True)
            OR (uc.isOrig = True AND uc.isDest = False) THEN cr.frequency
            ELSE 0
        END AS frequency,
        CASE
            WHEN (uc.isOrig = True AND uc.isDest = True)
            OR (uc.isOrig = True AND uc.isDest = False) THEN cr.monetaryValue
            ELSE 0
        END AS monetaryValue,
        CASE
            WHEN fa.CustomerID IS NOT NULL THEN True ELSE False
        END AS isFraudAccount,
        CASE
            WHEN sa.CustomerID IS NOT NULL THEN True ELSE False
        END AS isSuspectAccount,
        CASE
            WHEN va.CustomerID IS NOT NULL THEN True ELSE False
        END AS isVictimAccount
    FROM unique_customers uc
    LEFT JOIN customer_rfm cr ON uc.customerID = cr.nameOrig
    LEFT JOIN fraudulent_activity fa ON uc.customerID = fa.customerID
    LEFT JOIN suspect_activity sa ON uc.customerID = sa.customerID
    LEFT JOIN victim_activity va ON uc.customerID = va.customerID
    GROUP BY uc.customerID, uc.isOrig, uc.isDest, cr.recencyDays, cr.frequency, cr.monetaryValue, fa.CustomerID, sa.CustomerID, va.CustomerID
)

SELECT *,
    CASE 
        WHEN (isFraudAccount = True AND isSuspectAccount=False) OR (isFraudAccount = True AND isSuspectAccount=True) THEN 'Fraudster'
        WHEN isFraudAccount = False AND isSuspectAccount=True THEN 'Fraud Suspect'
        WHEN isVictimAccount = True THEN 'Victim'
        WHEN isFraudAccount = False AND isSuspectAccount = False THEN 'Not Affiliated'
    END AS customerStatus 
FROM customer_detail