WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__master_survey_questions') }}

    ),

    renamed AS (

        SELECT
            mastersurveyquestionid AS master_survey_question_id,
            stateid AS master_survey_question_state_id,
            cycle AS master_survey_question_cycle,
            mastersurveyquestiontype AS master_survey_question_type,
            mastersurveyquestionname AS master_survey_question,
            mastersurveyquestiontext AS master_survey_question_text,
            actiontypeid AS action_type_id,
            campaignid AS campaign_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||mastersurveyquestionid AS surrogate_master_survey_question_id


        FROM source

    )

SELECT * FROM renamed
