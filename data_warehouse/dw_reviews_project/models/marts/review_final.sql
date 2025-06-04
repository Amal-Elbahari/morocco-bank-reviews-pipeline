select
    review_text,
    cleaned_text,
    rating,
    review_date,
    bank_name,
    branch_name,
    location,
    lang,
    sentiment,
    topic
from {{ source('public', 'review_enriched') }}
