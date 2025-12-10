{{config(materialized='view')}}

select
    timestamp as execution_timestamp,
    metals.timestamp as metal_timestamp,
    news.timestamp as news_timestamp,
    metals.currency_base as currency_base,
    cast(metals.metals.gold as float64) as gold_price,
    cast(metals.metals.nickel as float64) as nickel_price,
    cast(metals.metals.copper as float64) as copper_price,
    cast(metals.metals.platinum as float64) as platinum_price,
    cast(metals.metals.silver as float64) as silver_price,
    cast(news.gold as int64) as gold_news_count,
    cast(news.nickel as int64) as nickel_news_count,
    cast(news.copper as int64) as copper_news_count,
    cast(news.platinum as int64) as platinum_news_count,
    cast(news.silver as int64) as silver_news_count,
from {{ source('commodity', 'commodity_data') }}
