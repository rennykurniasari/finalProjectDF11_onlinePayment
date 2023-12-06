with transactions as (

    select * from {{ ref('bronze_fraud') }}

)

select idTransaction, timestamp, amount, type, nameOrigin, nameDest, refund
from transactions
where isFraud = 1 and isTrueFraud = 1