

with source_data as (
    select distinct bank_name
    from {{ ref('review_final') }}  -- C'est la table qui contient les avis
)
select
    row_number() over () as bank_id,
    bank_name
from source_data

