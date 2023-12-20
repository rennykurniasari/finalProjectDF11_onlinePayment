
  
    

    create or replace table `final-project-404104`.`online_payment_marts`.`customer_detail`
      
    
    

    OPTIONS()
    as (
      WITH customer_detail AS (
  SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_customer_detail`
),

customer_segmentation AS (
  SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_customer_segmentation`
)

SELECT
    cd.customerID,
    cd.customerCategory,
    cs.customerType,
    cd.customerStatus,
    cd.recencyDays,
    cd.frequency,
    cd.monetaryValue
FROM customer_detail cd
LEFT JOIN customer_segmentation cs ON cd.customerID = cs.customerID
ORDER BY customerID
    );
  