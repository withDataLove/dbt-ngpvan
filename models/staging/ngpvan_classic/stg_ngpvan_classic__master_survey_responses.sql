WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__master_survey_responses') }}

    ),

    renamed AS (

        SELECT
            mastersurveyquestionid AS master_survey_question_id,
            mastersurveyresponseid AS master_survey_response_id,
            mastersurveyresponsename AS master_survey_response,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||mastersurveyresponseid AS surrogate_master_survey_response_id

        FROM source

    )

SELECT * FROM renamed
