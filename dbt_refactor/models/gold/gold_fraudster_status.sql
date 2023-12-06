with fraudster as (

    select * from {{ source('final_project_silver', 'fraudster') }}

)

SELECT
  CASE 
    WHEN fraudAction = 1 THEN 'ReceiveOneTrx'
    WHEN fraudAction > 1 THEN 'ReceiveMultipleTrx'
  END AS fraudActionStatus,
  COUNT(*) as count
FROM fraudster
GROUP BY fraudActionStatus

