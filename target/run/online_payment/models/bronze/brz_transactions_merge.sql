

  create or replace view `final-project-404104`.`online_payment_raw`.`brz_transactions_merge`
  OPTIONS()
  as SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_1`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_2`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_3`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_4`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_5`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_6`
UNION ALL
SELECT * FROM `final-project-404104`.`online_payment`.`online_payment_7`
ORDER BY step;

