with cleaned as (
    select distinct
        lower(trim(review_text)) as review_text,
        regexp_replace(lower(review_text), '[^\w\s]', '', 'g') as cleaned_text,
        rating,
        review_date,
        bank_name,
        branch_name,
        location
    from {{ source('raw', 'rev') }}
    where review_text is not null
)
select * from cleaned




