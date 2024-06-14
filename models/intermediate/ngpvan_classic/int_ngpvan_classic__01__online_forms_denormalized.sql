{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

with forms as (
    select *
    from {{ ref('stg_ngpvan_classic__online_forms') }}
),

types as (
    select *
    from {{ ref('stg_ngpvan_classic__online_form_types') }}
)
select
    forms.online_form_id,
    types.online_form_type_id,
    types.online_form_type_name,
    forms.online_form_name,
    forms.online_form_url,
    forms.committee_id,
    forms.campaign_id,
    forms.event_id,
    forms.is_active,
    confirmation_email_is_enabled,
    is_recurring_email_enabled,
    forms.utc_online_form_created_at,
    forms.utc_online_form_modified_at,
    forms.surrogate_online_form_id,
    surrogate_online_form_type_id
from forms
join types using(surrogate_online_form_type_id)