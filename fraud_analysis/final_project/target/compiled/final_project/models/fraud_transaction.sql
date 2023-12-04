

SELECT idTransaction, isFraud, isTrueFraud, refund, confirm, newBalanceOriginSet, oldBalanceOrigin, timestamp, amount
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND isTrueFraud=1