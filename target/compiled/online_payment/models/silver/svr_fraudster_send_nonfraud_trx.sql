WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`
),

fraudulent_senders AS (
    SELECT
        DISTINCT nameDest
    FROM source
    WHERE isFraud = True
)

SELECT 
    s.nameDest as customerID,
    s.transactionID, 
    s.transactionDatetime
FROM 
    source s
JOIN 
    fraudulent_senders fs ON s.nameOrig = fs.nameDest
WHERE 
    s.isFraud = False