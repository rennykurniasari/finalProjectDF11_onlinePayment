
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`victim_status_after_fraud`
      
    
    

    OPTIONS()
    as (
      

WITH FraudulentAccounts AS (
    SELECT DISTINCT nameOrigin
    FROM `eternal-channel-400514`.`final_project`.`fraud`
),

RepeatVictimFraud AS (
    SELECT DISTINCT nameOrig
    FROM `eternal-channel-400514`.`final_project`.`trx_victim_after_fraud`
)

SELECT 
    fa.nameOrigin,
    CASE 
        WHEN rc.nameOrig IS NOT NULL THEN 'Repeat Transaction'
        ELSE 'Inactive Post-Fraud'
    END AS AccountActivityStatus
FROM FraudulentAccounts fa
LEFT JOIN RepeatVictimFraud rc ON fa.nameOrigin = rc.nameOrig
    );
  