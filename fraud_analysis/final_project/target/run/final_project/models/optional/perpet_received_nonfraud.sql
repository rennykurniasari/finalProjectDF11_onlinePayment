
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`perpet_received_nonfraud`
      
    
    

    OPTIONS()
    as (
      

WITH FraudulentTransactions AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud = 1
),
NonFraudulentTransactions AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud = 0
)

SELECT 
    nft.nameDest,
    COUNT(*) AS NonFraudulentTransactionCount
FROM 
    NonFraudulentTransactions nft
WHERE 
    nft.nameDest IN (SELECT nameDest FROM FraudulentTransactions)
GROUP BY 
    nft.nameDest
    );
  