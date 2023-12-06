WITH UniqueCustomers AS (
    SELECT DISTINCT nameOrigin AS CustomerID
    FROM {{ ref('silver_allTransaction') }}
    UNION DISTINCT
    SELECT DISTINCT nameDest
    FROM {{ ref('silver_allTransaction') }}
),
FraudulentActivity AS (
    SELECT nameDest as CustomerID
    FROM {{ ref('silver_fraudster') }}
),
SuspectActivity AS (
    SELECT DISTINCT nameOrigin AS CustomerID
    FROM {{ ref('silver_perpet_received_nonfraud') }}
    UNION DISTINCT
    SELECT DISTINCT nameDest
    FROM {{ ref('silver_perpet_send_nonfraud') }}
),
CustomerFraudStatus AS (
    SELECT 
        uc.CustomerID,
        CASE WHEN fa.CustomerID IS NOT NULL THEN 1 ELSE 0 END AS isFraudAccount,
        CASE WHEN sa.CustomerID IS NOT NULL THEN 1 ELSE 0 END AS isSuspectAccount
    FROM 
        UniqueCustomers uc
    LEFT JOIN 
        FraudulentActivity fa ON uc.CustomerID = fa.CustomerID
    LEFT JOIN 
        SuspectActivity sa ON uc.CustomerID = sa.CustomerID
)

select * from CustomerFraudStatus
