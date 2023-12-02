
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`fraud_transaction_ofAll`
      
    
    

    OPTIONS()
    as (
      

SELECT payment_id, isFraud, payment_datetime, amount, isFlaggedFraud
FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    );
  