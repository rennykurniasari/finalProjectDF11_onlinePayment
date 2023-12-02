
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`customer_status`
      
    
    

    OPTIONS()
    as (
      

WITH UniqueCustomers AS (
    SELECT DISTINCT nameOrig AS CustomerID
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    UNION DISTINCT
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
),
FraudulentActivity AS (
    SELECT DISTINCT nameDest AS CustomerID
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud = 1
),
SuspectActivity AS (
    SELECT DISTINCT nameOrig AS CustomerID
    FROM `eternal-channel-400514`.`final_project`.`perpet_received_nonfraud`
    UNION DISTINCT
    SELECT DISTINCT nameDest
    FROM `eternal-channel-400514`.`final_project`.`perpet_send_nonfraud`
),
CustomerFraudStatus AS (
    SELECT 
        uc.CustomerID,
        CASE WHEN fa.CustomerID IS NOT NULL THEN 1 ELSE 0 END AS isFraudAccount,
        CASE WHEN sa.CustomerID IS NOT NULL THEN 1 ELSE 0 END AS isSuspectAccount
    FROM 
        UniqueCustomers uc
    LEFT JOIN 
        FraudulentActivity fa ON uc.CustomerID = fa.CustomerID
    LEFT JOIN 
        SuspectActivity sa ON uc.CustomerID = sa.CustomerID
)

SELECT *,
  CASE 
    WHEN isFraudAccount = 1 AND isSuspectAccount=0 THEN 'Fraudsters'
    WHEN isFraudAccount = 0 AND isSuspectAccount=1 THEN 'FraudSuspects'
    WHEN isFraudAccount = 0 AND isSuspectAccount = 0 THEN 'NotAffiliated'
    WHEN isFraudAccount = 1 AND isSuspectAccount=1 THEN 'Fraudsters'
  END AS CustomerStatus 
FROM CustomerFraudStatus
    );
  