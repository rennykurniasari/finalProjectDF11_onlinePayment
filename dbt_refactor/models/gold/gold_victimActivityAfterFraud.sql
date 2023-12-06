with fraudVictim as (

    select * from {{ source('final_project_silver', 'fraudVictim') }}

)

SELECT
  CASE 
    WHEN transferAfterFraud is False THEN 'InactivePostFraud'
    WHEN transferAfterFraud is True THEN 'RepeatTransaction'
  END AS fraudVictimStatus,
  COUNT(*) as count
FROM fraudVictim
GROUP BY fraudVictimStatus
