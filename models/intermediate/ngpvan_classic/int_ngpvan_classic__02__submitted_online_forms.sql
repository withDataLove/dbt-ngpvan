with contact_forms as (
    select *
    from {{ ref('stg_ngpvan_classic__contacts_online_forms') }}
),

forms as (
    select *
    from {{ ref('int_ngpvan_classic__01__online_forms_enhanced') }}
)

select
    contact_forms.contacts_online_form_id,
    contact_forms.vanid,
    contact_forms.online_form_id,
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
    surrogate_online_form_id,
    forms.online_form_type_name,
    forms.online_form_name,
    forms.online_form_url,
    committee_id,
    is_active
from contact_forms
left join forms using(surrogate_online_form_id)
