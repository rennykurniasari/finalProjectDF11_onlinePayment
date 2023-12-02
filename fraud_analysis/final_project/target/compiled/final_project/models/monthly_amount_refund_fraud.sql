

SELECT idTransaction, isFraud, refund, newBalanceOriginSet
FROM `eternal-channel-400514`.`final_project`.`fraud`
WHERE isFraud=1 AND refund=1