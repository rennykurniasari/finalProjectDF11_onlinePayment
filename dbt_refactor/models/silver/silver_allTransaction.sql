with transactions as (

    select * from {{ ref('bronze_transaction') }}

)

select idTransaction, timestamp, type, amount, nameOrigin, nameDest, isFraud
from transactions