
  
    

    create or replace table `eternal-channel-400514`.`final_project`.`trx_victim_before_fraud`
      
    
    

    OPTIONS()
    as (
      

WITH FirstFraudInstance AS (
    SELECT
        nameOrig,
        MIN(payment_datetime) AS FirstFraudDate
    FROM
        `eternal-channel-400514`.`final_project`.`fill_transactionValid`
    WHERE
        isFraud = 1
    GROUP BY
        nameOrig
)

SELECT
    t.nameOrig,
    t.payment_datetime,
    t.isFraud
FROM
    `eternal-channel-400514`.`final_project`.`fill_transactionValid` t
INNER JOIN
    FirstFraudInstance f ON t.nameOrig = f.nameOrig
WHERE
    t.payment_datetime < f.FirstFraudDate
ORDER BY
    t.nameOrig,
    t.payment_datetime
    );
  