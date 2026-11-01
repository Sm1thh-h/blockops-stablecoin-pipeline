{{
  config(
    materialized='view',
    tags=['staging', 'ethereum']
  )
}}

SELECT
    block_timestamp,
    block_number,
    transaction_hash,
    from_address,
    to_address,
    token_address,
    SAFE_CAST(value AS NUMERIC) AS transfer_amount_raw,
    'ethereum' AS chain
FROM {{ source('ethereum', 'token_transfers') }}
WHERE
    token_address IN (
        LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'),  -- USDC
        LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7')   -- USDT
    )
    AND DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)