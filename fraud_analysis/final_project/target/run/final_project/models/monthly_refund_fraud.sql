
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`monthly_refund_fraud`
      
    
    

    OPTIONS()
    as (
      

SELECT idTransaction, isFraud, refund
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND refund=1
    );
  