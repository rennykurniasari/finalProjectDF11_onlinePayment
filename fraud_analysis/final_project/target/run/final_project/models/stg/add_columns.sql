

  create or replace view `eternal-channel-400514`.`final_project`.`add_columns`
  OPTIONS()
  as 

SELECT
    *,
    '' as isValid, 
    0 as newbalanceOrig_valid, 
    0 as newbalanceDest_valid 
FROM `eternal-channel-400514`.`final_project`.`all_data1`;

