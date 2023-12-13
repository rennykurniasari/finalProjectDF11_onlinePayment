{% macro trx_validation_decreaseNewOrig() %}
    (ABS(oldBalanceOrig - amount - newBalanceOrig) <= 0.01 OR
    ROUND(oldBalanceOrig - amount, 2) = ROUND(newBalanceOrig, 2)) AND
    (ABS(oldBalanceDest + amount - newBalanceDest) <= 0.01 OR
    ROUND(oldBalanceDest + amount, 2) = ROUND(newBalanceDest, 2))
{% endmacro %}

{% macro trx_validation_increaseNewOrig() %}
    (ABS(oldBalanceOrig + amount - newBalanceOrig) <= 0.01 OR
    ROUND(oldBalanceOrig + amount, 2) = ROUND(newBalanceOrig, 2)) AND
    (ABS(oldBalanceDest - amount - newBalanceDest) <= 0.01 OR
    ROUND(oldBalanceDest - amount, 2) = ROUND(newBalanceDest, 2))
{% endmacro %}

{% macro trx_validation_decreaseNewOrigValid() %}
    (ABS(oldBalanceOrig - amount - newBalanceOrigValid) <= 0.01 OR
    ROUND(oldBalanceOrig - amount, 2) = ROUND(newBalanceOrigValid, 2)) AND
    (ABS(oldBalanceDest + amount - newBalanceDestValid) <= 0.01 OR
    ROUND(oldBalanceDest + amount, 2) = ROUND(newBalanceDestValid, 2))
{% endmacro %}

{% macro trx_validation_increaseNewOrigValid() %}
    (ABS(oldBalanceOrig + amount - newBalanceOrigValid) <= 0.01 OR
    ROUND(oldBalanceOrig + amount, 2) = ROUND(newBalanceOrigValid, 2)) AND
    (ABS(oldBalanceDest - amount - newBalanceDestValid) <= 0.01 OR
    ROUND(oldBalanceDest - amount, 2) = ROUND(newBalanceDestValid, 2))
{% endmacro %}
