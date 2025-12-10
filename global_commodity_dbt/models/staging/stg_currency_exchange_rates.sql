{{config(materialized='view')}}

select
  currency.date as currency_date,
  currency.base as currency_base,
  currency.rates.IDR as currency_idr,
  currency.rates.EUR as currency_eur,
  currency.rates.JPY as currency_jpy,
  currency.rates.CNY as currency_cny
from {{ source('commodity', 'commodity_data') }}
