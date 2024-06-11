WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__contacts_online_forms_responses_integer') }}
),
renamed AS (
    SELECT
        contactsonlineformid AS contacts_online_form_id,
        onlineformquestionid AS online_form_question_id,
        responsevalue AS response_value,
        createdby AS created_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_online_form_response_created_at,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_online_form_response_modified_at,

        -- additional columns
        ngpvan_instance||'|'||contactsonlineformid AS surrogate_contacts_online_form_id,
        ngpvan_instance||'|'||onlineformquestionid AS surrogate_online_form_question_id

    FROM source
)
SELECT * FROM renamed
