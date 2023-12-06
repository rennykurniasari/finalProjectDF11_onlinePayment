with not_fraud_transaction as (

select 
    idTransaction,
    timestamp,
    nameOrigin,
    nameDest
from  {{ ref('silver_allTransaction') }}
where isFraud = 0
),

fraud_transaction as (

    select * from {{ ref('silver_fraudster') }}

)

SELECT 
    nft.nameOrigin,
    nft.nameDest,
    nft.timestamp,
    nft.idTransaction
FROM 
    not_fraud_transaction nft
INNER JOIN 
    fraud_transaction ft ON nft.nameOrigin = ft.nameDest