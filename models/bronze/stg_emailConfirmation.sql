SELECT * FROM {{ source('online_payment', 'email_confirmation') }}
ORDER BY idTransaction