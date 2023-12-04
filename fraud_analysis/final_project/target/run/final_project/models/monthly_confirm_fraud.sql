
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`monthly_confirm_fraud`
      
    
    

    OPTIONS()
    as (
      

SELECT idTransaction, isFraud, confirm
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND confirm=1
    );
  