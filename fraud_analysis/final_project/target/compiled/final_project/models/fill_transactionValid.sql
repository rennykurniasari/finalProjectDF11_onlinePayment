

WITH base_data AS (
    SELECT *,
           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND
               

(ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg - amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest + amount, 1) = ROUND(newbalanceDest, 1))

 THEN 'True'
               WHEN type = 'CASH_IN' AND
               

(ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg + amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest - amount, 1) = ROUND(newbalanceDest, 1))

 THEN 'True'
               ELSE 'False'

           END AS isValid,

           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND 

(ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg - amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest + amount, 1) = ROUND(newbalanceDest, 1))


               THEN ROUND(oldbalanceOrg - amount, 1)
               WHEN type = 'CASH_IN' AND 

(ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg + amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest - amount, 1) = ROUND(newbalanceDest, 1))


               THEN ROUND(oldbalanceOrg + amount, 1)
               ELSE newbalanceOrig

           END AS newbalanceOrig_valid,

           CASE
               WHEN type IN ('PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT') AND 

(ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg - amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest + amount, 1) = ROUND(newbalanceDest, 1))


               THEN ROUND(oldbalanceDest + amount, 1)
               WHEN type = 'CASH_IN' AND 

(ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg + amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest - amount, 1) = ROUND(newbalanceDest, 1))


               THEN ROUND(oldbalanceDest - amount, 1)            
               ELSE newbalanceDest

           END AS newbalanceDest_valid

    FROM `eternal-channel-400514`.`final_project`.`all_data`
)

SELECT *
FROM base_data