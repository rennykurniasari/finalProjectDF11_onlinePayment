
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`monthly_fraud`
      
    
    

    OPTIONS()
    as (
      

SELECT idTransaction, isFraud, isTrueFraud
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND istrueFraud=1
    );
  