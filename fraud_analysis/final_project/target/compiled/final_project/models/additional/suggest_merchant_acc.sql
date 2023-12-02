

SELECT nameDest, count (*) AS trx
FROM `eternal-channel-400514`.`final_project`.`fill_transactionValid`
WHERE isFraud=0 
GROUP BY nameDest
ORDER BY trx DESC 
LIMIT 100