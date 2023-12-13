WITH source AS (
    SELECT * FROM {{ ref('brz_transactions') }}
),

first_fraud_instance AS (
    SELECT
        nameOrig,
        MIN(transactionDatetime) AS firstFraudDate
    FROM
        source
    WHERE
        isFraud = True
    GROUP BY
        nameOrig
)

SELECT
    s.nameOrig as customerID,
    s.transactionID,
    s.transactionDatetime,
    s.isFraud
FROM
    source s
INNER JOIN
    first_fraud_instance ffi ON s.nameOrig = ffi.nameOrig
WHERE
    s.transactionDatetime < ffi.firstFraudDate
ORDER BY
    s.nameOrig,
    s.transactionDatetime