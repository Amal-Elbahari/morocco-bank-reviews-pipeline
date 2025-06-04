with base as (
    select *,
        case
            when rating like '5%' or rating like '4%' then 'Positive'
            when rating like '1%' or rating like '2%' then 'Negative'
            else 'Neutral'
        end as sentiment
    from {{ ref('stg_reviews') }}
)

select * from base
