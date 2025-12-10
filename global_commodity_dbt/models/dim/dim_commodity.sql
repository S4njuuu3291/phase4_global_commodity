{{ config(materialized='table', on_schema_change='sync_all_columns') }}

WITH metals AS (
    SELECT 'gold' AS commodity_id, 'Gold' AS commodity_name, 'precious' AS commodity_type, 'USD/g' AS unit
    UNION ALL SELECT 'silver', 'Silver', 'precious', 'USD/g'
    UNION ALL SELECT 'platinum', 'Platinum', 'precious', 'USD/g'
    UNION ALL SELECT 'nickel', 'Nickel', 'industrial', 'USD/g'
    UNION ALL SELECT 'copper', 'Copper', 'industrial', 'USD/g'
)

SELECT
    commodity_id,
    commodity_name,
    commodity_type,
    unit,
    'USD' AS price_currency,
    'API' AS source
FROM metals
