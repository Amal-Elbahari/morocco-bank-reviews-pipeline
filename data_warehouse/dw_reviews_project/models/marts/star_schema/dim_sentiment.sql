with source_data as (
    select distinct sentiment
    from {{ ref('review_final') }}
)

select
    row_number() over () as sentiment_id,
    sentiment
from source_data

