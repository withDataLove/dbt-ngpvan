WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__online_forms') }}
),
renamed AS (
    SELECT
        onlineformid AS online_form_id,
        onlineformname AS online_form_name,
        onlineformurl AS online_form_url,
        onlineformtypeid AS online_form_type_id,
        committeeid AS committee_id,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_online_form_created_at,
        createdbydisplayname AS created_by_display_name,
        CONVERT_TIMEZONE('America/New_York', 'UTC', onlineformdatemodified) AS utc_online_form_modified_at,
        modifiedbydisplayname AS modified_by_display_name,
        isactive AS is_active,
        financialprogramid AS financial_program_id,
        confirmationemailfromaddress AS confirmation_email_from_address,
        confirmationemailfromname AS confirmation_email_from_name,
        confirmationemailsubject AS confirmation_email_subject,
        confirmationemailreplytoaddress AS confirmation_email_reply_to_address,
        confirmationemailcopytoaddresses AS confirmation_email_copy_to_addresses,
        confirmationemailcontenttext AS confirmation_email_content_text,
        confirmationemailisenabled AS confirmation_email_is_enabled,
        isrecurringemailenabled AS is_recurring_email_enabled,
        campaignid AS campaign_id,
        eventid AS event_id,
        userfacingonlineformurl AS user_facing_online_form_url,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,

        -- additional columns
        ngpvan_instance||'|'||onlineformid AS surrogate_online_form_id,
        ngpvan_instance||'|'||onlineformtypeid AS surrogate_online_form_type_id

    FROM source
)
SELECT * FROM renamed
