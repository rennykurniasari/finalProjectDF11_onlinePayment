WITH source AS (
  SELECT * FROM `final-project-404104`.`online_payment_transformed`.`svr_balancing_transactions`
),

valid_calculation AS(
    SELECT
        type,
        ROUND(SUM(newbalanceOrig), 2) as totalNewbalanceOrig,
        ROUND(SUM(newbalanceOrigValid), 2) as totalNewbalanceOrigValid,
        ROUND(SUM(newbalanceDest), 2) as totalNewbalanceDest,
        ROUND(SUM(newbalanceDestValid), 2) as totalNewbalanceDestValid
    FROM source
    GROUP BY type
)

SELECT
    type,
    totalNewbalanceOrig,
    totalNewbalanceOrigValid,
    ROUND(totalNewbalanceOrigValid - totalNewbalanceOrig, 2) as diffNewbalanceOrig,
    totalNewbalanceDest,
    totalNewbalanceDestValid,
    ROUND(totalNewbalanceDestValid - totalNewbalanceDest, 2) as diffNewbalanceDest
FROM valid_calculation