with customer as (

    select * from {{ ref('silver_customer') }}

)

SELECT
  CASE 
    WHEN isFraudAccount = 1 AND isSuspectAccount=0 THEN 'Fraudsters'
    WHEN isFraudAccount = 0 AND isSuspectAccount=1 THEN 'FraudSuspects'
    WHEN isFraudAccount = 0 AND isSuspectAccount = 0 THEN 'NotAffiliated'
    WHEN isFraudAccount = 1 AND isSuspectAccount=1 THEN 'Fraudsters'
  END AS customerStatus,
  COUNT(*) as count
FROM customer
GROUP BY customerStatus