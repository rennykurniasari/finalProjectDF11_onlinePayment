WITH source AS (
    SELECT * FROM {{ ref('brz_transactions') }}
),

fraudulent_transactions AS (
    SELECT
        DISTINCT nameDest
    FROM source
    WHERE isFraud = True
)

SELECT 
    nft.nameDest as customerID,
    nft.transactionID,
    nft.transactionDatetime,
    COUNT(*) AS nonFraudulentTransactionCount
FROM 
    source nft
INNER JOIN 
    fraudulent_transactions ft ON nft.nameDest = ft.nameDest
WHERE 
    nft.isFraud = False 
GROUP BY 
    nft.nameDest,
    nft.transactionID,
    nft.transactionDatetime
ORDER BY
    nft.transactionDatetime