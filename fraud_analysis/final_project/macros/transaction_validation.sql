{% macro trx_validation_decreaseNewOrig() %}

(ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg - amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest + amount, 1) = ROUND(newbalanceDest, 1))

{% endmacro %}

{% macro trx_validation_increaseNewOrig() %}

(ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01 OR
ROUND(oldbalanceOrg + amount, 1) = ROUND(newbalanceOrig, 1)) AND
(ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01 OR
ROUND(oldbalanceDest - amount, 1) = ROUND(newbalanceDest, 1))

{% endmacro %}