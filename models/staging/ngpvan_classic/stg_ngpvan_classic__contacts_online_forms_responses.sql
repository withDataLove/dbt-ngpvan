{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__contacts_online_forms_responses') }}
),
renamed AS (
    SELECT
        contactsonlineformid||'|'||onlineformquestionid||'|'||NVL(onlineformresponseid::TEXT, MD5(responsevalue), 'unknown') as contacts_online_forms_response_id,
        contactsonlineformid AS contacts_online_form_id,
        onlineformquestionid AS online_form_question_id,
        onlineformresponseid AS online_form_response_id,
        responsevalue AS response_value,
        otherresponsevalue AS other_response_value,
        createdby AS created_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_online_form_response_created_at,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_online_form_response_modified_at,

        -- additional columns
        ngpvan_instance||'|'||contactsonlineformid||'|'||onlineformquestionid||'|'||NVL(onlineformresponseid::TEXT, MD5(responsevalue), 'unknown') as surrogate_contacts_online_forms_response_id,
        ngpvan_instance||'|'||contactsonlineformid AS surrogate_contacts_online_form_id,
        ngpvan_instance||'|'||onlineformquestionid AS surrogate_online_form_question_id,
        ngpvan_instance||'|'||onlineformresponseid AS surrogate_online_form_response_id

    FROM source
)
SELECT * FROM renamed
