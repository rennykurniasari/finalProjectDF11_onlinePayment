
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`perpet_received_fraud`
      
    
    

    OPTIONS()
    as (
      

SELECT DISTINCT payment_id, nameDest, payment_datetime, isFraud
FROM 
  ( SELECT *, 
           count(1) OVER (PARTITION BY nameDest) AS occurs
    FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE isFraud=1
  ) AS t 
WHERE occurs > 1
ORDER BY nameDest, payment_datetime
    );
  