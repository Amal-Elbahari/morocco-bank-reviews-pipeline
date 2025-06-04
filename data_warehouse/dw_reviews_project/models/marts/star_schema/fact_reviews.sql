with review_source as (
    select *
    from {{ ref('review_final') }}
),
bank as (
    select *
    from {{ ref('dim_bank') }}
),
branch as (
    select *
    from {{ ref('dim_branch') }}
),
location as (
    select *
    from {{ ref('dim_location') }}
),
sentiment as (
    select *
    from {{ ref('dim_sentiment') }}
)

select
    row_number() over () as review_id,
    r.review_text,
    r.cleaned_text,
    case
        when r.rating like '1%' then 1
        when r.rating like '2%' then 2
        when r.rating like '3%' then 3
        when r.rating like '4%' then 4
        when r.rating like '5%' then 5
        else null
    end as rating,
    r.review_date,
    b.bank_id,
    br.branch_id,
    l.location_id,
    s.sentiment_id,
    r.topic
from review_source r
left join bank b on r.bank_name = b.bank_name
left join branch br on r.branch_name = br.branch_name
left join location l on r.location = l.city   -- 🡐 ici on JOIN sur city !
left join sentiment s on r.sentiment = s.sentiment

