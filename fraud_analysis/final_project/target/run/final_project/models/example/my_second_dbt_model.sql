

  create or replace view `eternal-channel-400514`.`final_project`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `eternal-channel-400514`.`final_project`.`my_first_dbt_model`
where id = 1;

