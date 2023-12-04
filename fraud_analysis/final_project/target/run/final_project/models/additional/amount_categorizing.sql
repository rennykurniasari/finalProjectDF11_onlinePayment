

  create or replace view `eternal-channel-400514`.`final_project`.`amount_categorizing`
  OPTIONS()
  as SELECT
  type,
  CASE
    WHEN amount <= 200000 THEN '0-200K'
    WHEN amount > 200000 AND amount <= 400000 THEN '200K-400K'
    WHEN amount > 400000 AND amount <= 600000 THEN '400K-600K'
    WHEN amount > 600000 AND amount <= 800000 THEN '600K-800K'
    WHEN amount > 800000 AND amount <= 1000000 THEN '800K-1,000K'
    WHEN amount > 1000000 THEN 'More than 1,000K'
  END AS amount_range,
  COUNT(*) AS transaction_count, 
  SUM(amount) AS total_amount
FROM
  `eternal-channel-400514`.`final_project`.`fill_transactionValid`
GROUP BY
  type,
  amount_range
ORDER BY amount_range;

