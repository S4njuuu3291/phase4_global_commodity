{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

WITH currencies AS (
    SELECT 'USD' AS currency_code, 'US Dollar' AS currency_name, 'United States' AS region, TRUE AS is_base_reference
    UNION ALL SELECT 'IDR', 'Indonesian Rupiah', 'Indonesia', FALSE
    UNION ALL SELECT 'EUR', 'Euro', 'European Union', FALSE
    UNION ALL SELECT 'JPY', 'Japanese Yen', 'Japan', FALSE
    UNION ALL SELECT 'CNY', 'Chinese Yuan', 'China', FALSE
)

SELECT
    currency_code,
    currency_name,
    region,
    is_base_reference,
    'API' AS source
FROM currencies
