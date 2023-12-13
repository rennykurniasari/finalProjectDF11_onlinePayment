
  
    

    create or replace table `final-project-404104`.`online_payment_transformed`.`svr_repeated_fraudsters`
      
    
    

    OPTIONS()
    as (
      WITH source AS (

    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_transactions`

)

SELECT
    DISTINCT nameDest as customerID,
    occurs as fraudOccurs
FROM 
  ( SELECT *, 
           count(transactionID) OVER (PARTITION BY nameDest) AS occurs
    FROM source
    WHERE isFraud=True
  ) AS t 
WHERE occurs > 1
ORDER BY customerID
    );
  