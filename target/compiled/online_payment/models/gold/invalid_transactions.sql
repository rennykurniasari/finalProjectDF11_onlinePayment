WITH source AS (
  SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_balancing_transactions`
)

SELECT
    transactionID,
    transactionDatetime,
    CASE
        WHEN EXTRACT(HOUR FROM transactionDatetime) >= 21 OR EXTRACT(HOUR FROM transactionDatetime) < 5 THEN 'Night'
        WHEN EXTRACT(HOUR FROM transactionDatetime) >= 5 AND EXTRACT(HOUR FROM transactionDatetime) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM transactionDatetime) >= 12 AND EXTRACT(HOUR FROM transactionDatetime) < 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM transactionDatetime) >= 17 AND EXTRACT(HOUR FROM transactionDatetime) < 21 THEN 'Evening'
    END AS timePeriod,
    FORMAT_DATE('%A', transactionDatetime) AS day,
    CASE 
        WHEN EXTRACT(DAY FROM transactionDatetime) <= 7 THEN 'Week 1'
        WHEN EXTRACT(DAY FROM transactionDatetime) <= 14 THEN 'Week 2'
        WHEN EXTRACT(DAY FROM transactionDatetime) <= 21 THEN 'Week 3'
        WHEN EXTRACT(DAY FROM transactionDatetime) <= 28 THEN 'Week 4'
        ELSE 'Week 5'
    END AS weekOfMonth,
    type,
    amount,
    nameOrig,
    oldBalanceOrig,
    newbalanceOrig,
    newBalanceOrigValid,
    nameDest,
    oldbalanceDest,
    newbalanceDest,
    newBalanceDestValid,
    isValid,
    isValidUpdate,
    isFraud,
    isFlaggedFraud
FROM source
WHERE isValid = False
ORDER BY transactionDatetime