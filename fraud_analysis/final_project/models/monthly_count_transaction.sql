-- Jika Anda ingin menggunakan tabel materialized
{{ config(
  materialized='table'
) }}

-- Kueri SQL untuk memproses data
WITH monthly_count_transaction AS (
  SELECT
    payment_datetime,
    isFraud,
    payment_id
  FROM
        {{ source('Finalproject', 'Fraud') }}
)

-- Pilih kolom yang Anda butuhkan
SELECT
  payment_datetime,
  isFraud,
  payment_id
FROM monthly_count_transaction
