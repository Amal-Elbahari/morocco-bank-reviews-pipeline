-- models/dim_location.sql

with source_data as (
    select distinct location
    from {{ ref('review_final') }}
)
select
    row_number() over () as location_id,
    location as city -- You can split the location if needed
from source_data

