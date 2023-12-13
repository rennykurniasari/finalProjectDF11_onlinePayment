
  
    

    create or replace table `final-project-404104`.`online_payment_transformed`.`svr_fraud_detail`
      
    
    

    OPTIONS()
    as (
      WITH source AS (
    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_fraud_confirmation`
)

SELECT
    transactionID,
    transactionDatetime,
    type,
    amount,
    isFraud,
    isFlaggedFraud,
    isConfirmed,
    isTrueFraud,
    nameOrig,
    oldBalanceOrig,
    newBalanceOrig,
    nameDest,
    oldBalanceDest,
    newBalanceDest
FROM
    source
ORDER BY transactionID
    );
  