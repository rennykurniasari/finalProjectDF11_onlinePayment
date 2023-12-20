
  
    

    create or replace table `final-project-404104`.`online_payment_transformed`.`svr_refund_detail`
      
    
    

    OPTIONS()
    as (
      WITH source AS (

    SELECT * FROM `final-project-404104`.`online_payment_raw`.`brz_fraud_confirmation`

)

SELECT
    transactionID,
    transactionDatetime,
    type,
    isRefunded,
    amount as refundedAmount,
    oldBalanceOrigSet,
    newBalanceOrigSet,
    oldBalanceDestSet,
    newBalanceDestSet
FROM
    source
ORDER BY transactionID
    );
  