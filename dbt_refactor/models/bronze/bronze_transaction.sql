with source as (

    select * from {{ source('final_project', 'transaction') }}

)

select * from source