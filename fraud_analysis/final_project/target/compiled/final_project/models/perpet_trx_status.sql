

WITH FraudulentTransactions AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fraud`
),

MultipleTransactions AS (
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`perpet_received_fraud`
)

SELECT 
    f.nameDest,
    CASE 
        WHEN f.nameDest IN (SELECT nameDest FROM MultipleTransactions) THEN 'ReceiveMultipleTrx'
        WHEN f.nameDest NOT IN (SELECT nameDest FROM MultipleTransactions) THEN 'ReceiveOneTrx'
    END AS TransactionCategory
FROM 
    FraudulentTransactions f