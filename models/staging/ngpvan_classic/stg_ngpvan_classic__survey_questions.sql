WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__survey_questions') }}

    ),

    renamed AS (

        SELECT
            surveyquestionid AS survey_question_id,
            stateid AS survey_question_state_id,
            cycle AS survey_question_cycle,
            surveyquestiontype AS survey_question_type,
            surveyquestionname AS survey_question,
            surveyquestiontext AS survey_question_text,
            mastersurveyquestionid AS master_survey_question_id,
            createdcommitteeid AS created_by_committee_id,
            active AS survey_question_status,
            CASE WHEN active = 1 THEN TRUE ELSE FALSE END AS is_active,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            CASE WHEN mastersurveyquestionid IS NOT NULL THEN TRUE ELSE FALSE END AS has_master_survey_question,
            ngpvan_instance||'|'||surveyquestionid AS surrogate_survey_question_id,
            ngpvan_instance||'|'||mastersurveyquestionid AS surrogate_master_survey_question_id

        FROM source

    )

SELECT * FROM renamed