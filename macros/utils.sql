{% macro balancing_transaction() %}
    UPDATE {{ ref('svr_imbalance_transactions') }}
    SET newbalanceOrig = 
        CASE
            WHEN type = 'PAYMENT' THEN
            CASE
                WHEN ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 THEN newbalanceOrig - amount
                ELSE oldbalanceOrg
            END
            WHEN type = 'TRANSFER' THEN
            CASE
                WHEN (ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01) OR (ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01) THEN newbalanceOrig - amount
                ELSE oldbalanceOrg
            END
            WHEN type = 'CASH_OUT' THEN
            CASE
                WHEN (ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01) OR (ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01) OR ((oldbalanceOrg < amount) AND (newbalanceOrig = 0)) THEN newbalanceOrig - amount + newbalanceDest
                ELSE oldbalanceOrg
            END
            WHEN type = 'CASH_IN' THEN
            CASE
                WHEN (ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01) OR (ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01) OR ((oldbalanceDest < amount) AND (newbalanceDest = 0)) THEN newbalanceOrig + amount
                ELSE oldbalanceOrg
            END
            WHEN type = 'DEBIT' THEN
            CASE
                WHEN ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 THEN newbalanceOrig + amount
                ELSE oldbalanceOrg
            END
            ELSE oldbalanceOrg
        END,
        newbalanceDest = 
        CASE
            WHEN type = 'PAYMENT' THEN
            CASE
                WHEN ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 THEN newbalanceDest
                ELSE oldbalanceDest + amount
            END
            WHEN type = 'TRANSFER' THEN
            CASE
                WHEN (ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01) OR (ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01) THEN newbalanceDest + amount
                ELSE oldbalanceDest
            END
            WHEN type = 'CASH_OUT' THEN
            CASE
                WHEN (ABS(oldbalanceDest + amount - newbalanceDest) <= 0.01) OR (ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01) OR ((oldbalanceOrg < amount) AND (newbalanceOrig = 0)) THEN newbalanceDest - amount
                ELSE oldbalanceDest
            END
            WHEN type = 'CASH_IN' THEN
            CASE
                WHEN (ABS(oldbalanceOrg + amount - newbalanceOrig) <= 0.01) OR (ABS(oldbalanceDest - amount - newbalanceDest) <= 0.01) OR ((oldbalanceDest < amount) AND (newbalanceDest = 0)) THEN newbalanceDest - amount + newbalanceOrig
                ELSE oldbalanceDest
            END
            WHEN type = 'DEBIT' THEN
            CASE
                WHEN ABS(oldbalanceOrg - amount - newbalanceOrig) <= 0.01 THEN newbalanceDest
                ELSE oldbalanceDest
            END
            ELSE oldbalanceDest
        END
    WHERE valid_transaction = True
{% endmacro %}