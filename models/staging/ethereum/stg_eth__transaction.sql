{{
  config(
    materialized='view',
    tags=['staging', 'ethereum']
  )
}}

SELECT
    'hash' AS transaction_hash,
    block_number,
    block_timestamp,
    from_address AS tx_sender,
    to_address AS tx_called_contract,
    receipt_gas_used AS gas_used,
    receipt_effective_gas_price AS effective_gas_price_wei,
    -- NUMERIC avoids INT64 overflow on large gas_used x gas_price products
    CAST(receipt_gas_used AS NUMERIC)
      * CAST(receipt_effective_gas_price AS NUMERIC) as fee_wei,
    CAST(receipt_gas_used AS NUMERIC)
      * CAST(receipt_effective_gas_price AS NUMERIC) / 1e18 AS fee_eth
from {{ source('ethereum', 'transactions') }}
where receipt_status = 1
  AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)