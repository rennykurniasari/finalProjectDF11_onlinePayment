with source as (

    select * from {{ source('final_project', 'fraud') }}

)

select * from source