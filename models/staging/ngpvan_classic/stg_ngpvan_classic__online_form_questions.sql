{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__online_form_questions') }}
),

renamed AS (
    SELECT
        onlineformquestionid AS online_form_question_id,
        onlineformid AS online_form_id,
        onlineformquestionname AS online_form_question_name,
        onlineformquestiondisplayname AS online_form_question_display_name,
        onlineformquestiontypeid AS online_form_question_type_id,
        isinterest AS is_interest,
        isdefault AS is_default,
        customfieldid AS custom_field_id,
        orderby AS order_by,
        createdby AS created_by,
        modifiedby AS modified_by,
        suppressedby AS suppressed_by,
        activistcodeid AS activist_code_id,
        surveyquestionid AS survey_question_id,
        CONVERT_TIMEZONE(
            'America/New_York', 'UTC', datecreated
        ) AS utc_online_form_question_created_at,
        CONVERT_TIMEZONE(
            'America/New_York', 'UTC', datemodified
        ) AS utc_online_form_question_modified_at,
        CAST(isarchived AS BOOLEAN) AS is_question_archived,
        CONVERT_TIMEZONE(
            'America/New_York', 'UTC', datesuppressed
        ) AS utc_online_form_question_suppressed_at,

        -- additional columns
        (
            ngpvan_instance
            || '|'
            || onlineformquestionid
        ) AS surrogate_online_form_question_id,
        (ngpvan_instance || '|' || onlineformid) AS surrogate_online_form_id,
        (
            ngpvan_instance
            || '|'
            || onlineformquestiontypeid
        ) AS surrogate_online_form_question_type_id,
        (
            ngpvan_instance || '|' || activistcodeid
        ) AS surrogate_activist_code_id,
        (
            ngpvan_instance
            || '|'
            || surveyquestionid
        ) AS surrogate_survey_question_id

    FROM source
)

SELECT * FROM renamed
