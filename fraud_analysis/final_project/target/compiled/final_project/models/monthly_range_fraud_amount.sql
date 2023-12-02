

SELECT idTransaction, isFraud, isTrueFraud, timestamp, amount
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND istrueFraud=1