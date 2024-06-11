WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__online_form_responses') }}
),
renamed AS (
    SELECT
        onlineformresponseid AS online_form_response_id,
        onlineformquestionid AS online_form_question_id,
        onlineformresponsename AS online_form_response_name,
        onlineformresponsedisplayname AS online_form_response_display_name,
        isotherresponse AS is_other_response,
        customfieldid AS custom_field_id,
        customfieldvalueid AS custom_field_value_id,
        activistcodeid AS activist_code_id,
        createdby AS created_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_online_form_response_created_at,
        modifiedby AS modified_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_online_form_response_modified_at,
        isarchived AS is_archived,
        suppressedby AS suppressed_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datesuppressed) AS utc_online_form_response_suppressed_at,
        isnote AS is_note,
        surveyquestionid AS survey_question_id,
        surveyresponseid AS survey_response_id,

        -- additional columns
        ngpvan_instance||'|'||onlineformresponseid AS surrogate_online_form_response_id,
        ngpvan_instance||'|'||onlineformquestionid AS surrogate_online_form_question_id,
        ngpvan_instance||'|'||surveyquestionid AS surrogate_survey_question_id,
        ngpvan_instance||'|'||surveyresponseid AS surrogate_survey_response_id

    FROM source
)
SELECT * FROM renamed
