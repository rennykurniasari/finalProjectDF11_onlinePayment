WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`
),

customer_rfm AS (
    SELECT
        nameOrig AS customerID,
        DATE_DIFF(DATE(CURRENT_DATETIME()), DATE(MAX(transactionDatetime)), DAY) AS recencyDays,
        COUNT(*) AS frequency,
        ROUND(SUM(amount), 2) AS monetaryValue
    FROM source
    GROUP BY nameOrig
),

fraudulent_activity AS (
    SELECT DISTINCT nameDest AS customerID
    FROM source
    WHERE isFraud = True
),

suspect_activity AS (
    SELECT DISTINCT nameOrig AS customerID
    FROM source
    WHERE (nameDest IN (SELECT customerID FROM `final-project-404104`.`online_payment_transformed`.`svr_fraudster_receive_nonfraud_trx`) AND isFraud = False)

    UNION DISTINCT
    
    SELECT DISTINCT customerID
    FROM `final-project-404104`.`online_payment_transformed`.`svr_fraudster_send_nonfraud_trx`
),

victim_activity AS (
    SELECT DISTINCT nameOrig AS customerID
    FROM source
    WHERE isFraud = True
),

unique_customers AS (
    SELECT
        DISTINCT nameOrig as customerID
    FROM source
    UNION ALL
    SELECT
        DISTINCT nameDest as customerID
    FROM source
),

customer_detail AS (
    SELECT
        DISTINCT uc.customerID,
        CASE WHEN os.nameOrig = ds.nameDest THEN True ELSE False END AS isOrig,
        CASE WHEN os.nameOrig <> ds.nameDest THEN True ELSE False END AS isDest,
        CASE
            WHEN (os.nameOrig = ds.nameDest OR os.nameOrig <> ds.nameDest) THEN cr.recencyDays
            ELSE DATE_DIFF(DATE(CURRENT_DATETIME()), DATE('2022-12-31'), DAY)
        END AS recencyDays,
        CASE
            WHEN (os.nameOrig = ds.nameDest OR os.nameOrig <> ds.nameDest) THEN cr.frequency
            ELSE 0
        END AS frequency,
        CASE
            WHEN (os.nameOrig = ds.nameDest OR os.nameOrig <> ds.nameDest) THEN cr.monetaryValue
            ELSE 0
        END AS monetaryValue,
        
        CASE WHEN fa.CustomerID IS NOT NULL THEN True ELSE False END AS isFraudAccount,
        CASE WHEN sa.CustomerID IS NOT NULL THEN True ELSE False END AS isSuspectAccount,
        CASE WHEN va.CustomerID IS NOT NULL THEN True ELSE False END AS isVictimAccount
    FROM unique_customers uc
    LEFT JOIN customer_rfm cr ON uc.customerID = cr.customerID
    LEFT JOIN source os ON os.nameOrig = uc.customerID
    LEFT JOIN source ds ON ds.nameDest = uc.customerID
    LEFT JOIN fraudulent_activity fa ON uc.customerID = fa.customerID
    LEFT JOIN suspect_activity sa ON uc.customerID = sa.CustomerID
    LEFT JOIN victim_activity va ON uc.customerID = va.CustomerID
)

SELECT
    customerID,
    isOrig,
    isDest,
    CASE 
        WHEN isOrig = True AND isDest = False THEN 'Sender'
        WHEN isOrig = False AND isDest = True THEN 'Receiver'
        WHEN isOrig = True AND isDest = True THEN 'Transactor'
    END AS customerCategory,
    recencyDays,
    frequency,
    monetaryValue,
    isFraudAccount,
    isSuspectAccount,
    isVictimAccount,
    CASE 
        WHEN (isFraudAccount = True AND isSuspectAccount=False) OR (isFraudAccount = True AND isSuspectAccount=True) THEN 'Fraudster'
        WHEN isFraudAccount = False AND isSuspectAccount=True THEN 'Fraud Suspect'
        WHEN isVictimAccount = True THEN 'Victim'
        WHEN isFraudAccount = False AND isSuspectAccount = False THEN 'Not Affiliated'
    END AS customerStatus 
FROM customer_detail