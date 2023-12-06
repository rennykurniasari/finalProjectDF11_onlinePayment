with fraud_transaction as (

    select * from {{ ref('silver_fraudTransaction') }}

),

FraudulentAccounts as (
select 
    nameOrigin,
    MIN(timestamp) AS firstFraudDate,
    MAX(timestamp) AS lastFraudDate
from fraud_transaction
GROUP BY
    nameOrigin
),

RepeatVictimFraud AS (
SELECT
    t.nameOrigin,
    t.timestamp
FROM
    {{ ref('silver_allTransaction') }} t
INNER JOIN
    FraudulentAccounts f ON t.nameOrigin = f.nameOrigin
WHERE
    t.timestamp > f.FirstFraudDate and t.isFraud = 0
),

VictimStatusAfterFraud as (
SELECT 
    fa.nameOrigin,
    fa.firstFraudDate,
    CASE 
        WHEN rc.nameOrigin IS NOT NULL THEN True
        ELSE False
    END AS transferAfterFraud
FROM FraudulentAccounts fa
LEFT JOIN RepeatVictimFraud rc ON fa.nameOrigin = rc.nameOrigin
)

SELECT * FROM VictimStatusAfterFraud