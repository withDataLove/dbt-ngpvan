{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

with source as (

    select * from {{ ref('base_ngpvan_classic__contacts_online_forms') }}

),

renamed as (

    select
        contactsonlineformid as contacts_online_form_id,
        vanid as vanid,
        onlineformid as online_form_id,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_contacts_online_form_created_at,
        isnewcontact as is_new_contact,
        submittedfirstname as submitted_first_name,
        submittedlastname as submitted_last_name,
        submittedaddressline1 as submitted_address_line1,
        submittedpostalcode as submitted_postal_code,
        submittedcity as submitted_city,
        submittedstateprovince as submitted_state_province,
        foreignsubmitlogid as foreign_submit_log_id,
        marketsource as market_source,
        batchemailjobdistributionid as batch_email_job_distribution_id,
        contactsnoteid as contacts_note_id,
        submittedhomeemail as submitted_home_email,
        submittedworkemail as submitted_work_email,
        submittedhomephone as submitted_home_phone,
        submittedhomephonecountrycode as submitted_home_phone_country_code,
        submittedhomephoneinternationaldialingprefix as submitted_home_phone_international_dialing_prefix,
        submittedworkphone as submitted_work_phone,
        submittedworkphonecountrycode as submitted_work_phone_country_code,
        submittedworkphoneinternationaldialingprefix as submitted_work_phone_international_dialing_prefix,
        submittedmobilephone as submitted_mobile_phone,
        submittedmobilephonecountrycode as submitted_mobile_phone_country_code,
        submittedmobilephoneinternationaldialingprefix as submitted_mobile_phone_international_dialing_prefix,
        submittedtitle as submitted_title,
        submittedsuffix as submitted_suffix,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_contacts_online_form_modified_at,

        -- additional columns
        ngpvan_instance||'|'||contactsonlineformid AS surrogate_contacts_online_form_id,
        ngpvan_instance||'|'||vanid AS surrogate_van_id,
        ngpvan_instance||'|'||onlineformid AS surrogate_online_form_id


    from source

)

select * from renamed