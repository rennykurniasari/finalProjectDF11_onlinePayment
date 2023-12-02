

WITH FraudulentSenders AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud = 1
)

SELECT 
    a.payment_id, 
    a.payment_datetime, 
    a.nameOrig, 
    a.nameDest
FROM 
    `eternal-channel-400514`.`final_project`.`fill_transactionValid` a
JOIN 
    FraudulentSenders fs ON a.nameOrig = fs.nameDest
WHERE 
    a.isFraud = 0