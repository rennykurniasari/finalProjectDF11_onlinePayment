-- Jika Anda ingin menggunakan tabel materialized
{{ config(
  materialized='table'
) }}

-- Kueri SQL untuk memproses data
WITH monthly_amount_transaction AS (
  SELECT
    payment_datetime,
    isFraud,
    payment_id,
    amount
  FROM
        {{ source('Finalproject', 'Fraud') }}
)

-- Pilih kolom yang Anda butuhkan
SELECT
  payment_datetime,
  isFraud,
  payment_id,
  amount
FROM monthly_amount_transaction