with submitted_responses as (
    select * from {{ ref('stg_ngpvan_classic__contacts_online_forms_responses') }}
)

select * from submitted_responses