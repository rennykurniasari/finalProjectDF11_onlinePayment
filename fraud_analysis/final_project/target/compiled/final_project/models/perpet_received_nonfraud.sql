

WITH FraudulentTransactions AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud = 1
)

SELECT 
    nft.nameOrig,
    nft.nameDest,
    nft.payment_datetime,
    nft.payment_id,
    COUNT(*) AS NonFraudulentTransactionCount
FROM 
    `eternal-channel-400514`.`final_project`.`fill_transactionValid` nft
INNER JOIN 
    FraudulentTransactions ft ON nft.nameDest = ft.nameDest
WHERE 
    nft.isFraud = 0 
GROUP BY 
    nft.nameOrig,
    nft.nameDest,
    nft.payment_datetime,
    nft.payment_id
ORDER BY
    nft.nameOrig,
    nft.payment_datetime