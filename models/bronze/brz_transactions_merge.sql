SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_1') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_2') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_3') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_4') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_5') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_6') }}
UNION ALL
SELECT * FROM {{ source('brz_online_payment', 'brz_online_payment_7') }}
ORDER BY step