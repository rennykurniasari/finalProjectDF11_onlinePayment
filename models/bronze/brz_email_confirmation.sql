SELECT * FROM {{ source('brz_online_payment', 'brz_email_confirmation') }}
ORDER BY idTransaction