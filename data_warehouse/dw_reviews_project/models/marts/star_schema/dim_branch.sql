-- models/dim_branch.sql

with source_data as (
    select distinct branch_name
    from {{ ref('review_final') }}
)
select
    row_number() over () as branch_id,
    branch_name
from source_data

