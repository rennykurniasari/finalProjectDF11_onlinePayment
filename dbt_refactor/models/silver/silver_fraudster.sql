with fraud_transaction as (

    select * from {{ ref('silver_fraudTransaction') }}

)

select distinct nameDest, count(nameDest) as fraudAction
from fraud_transaction
group by nameDest