

  create or replace view `final-project-404104`.`online_payment_raw`.`brz_email_confirmation`
  OPTIONS()
  as SELECT * FROM `final-project-404104`.`online_payment`.`email_confirmation`
ORDER BY idTransaction;

