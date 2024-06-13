with contact_forms as (
    select *
    from dbt_transformations.stg_ngpvan_classic__contacts_online_forms
),

forms as (
    select *
    from dbt_transformations.stg_ngpvan_classic__online_forms
)

select
    contact_forms.contacts_online_form_id,
    contact_forms.vanid,
    online_form_id,
    contact_forms.is_new_contact,
    contact_forms.submitted_title,
    contact_forms.submitted_first_name,
    contact_forms.submitted_last_name,
    contact_forms.submitted_suffix,
    contact_forms.submitted_address_line1,
    contact_forms.submitted_postal_code,
    contact_forms.submitted_city,
    contact_forms.submitted_state_province,
    contact_forms.submitted_home_email,
    contact_forms.submitted_mobile_phone,
    contact_forms.submitted_mobile_phone_country_code,
    contact_forms.submitted_mobile_phone_international_dialing_prefix,
    contact_forms.utc_contacts_online_form_created_at,
    contact_forms.utc_contacts_online_form_modified_at,
    contact_forms.surrogate_contacts_online_form_id,
    contact_forms.surrogate_van_id,
    contact_forms.surrogate_online_form_id,
    forms.*
from contact_forms
left join forms using(online_form_id)
where online_form_id = 504