WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__survey_responses') }}

    ),

    renamed AS (

        SELECT
            surveyquestionid AS survey_question_id,
            surveyresponseid AS survey_response_id,
            surveyresponsename AS survey_response,
            dempoints AS democratic_points,
            reppoints AS republican_points,
            indpoints AS independent_points,
            mastersurveyresponseid AS master_survey_response_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||surveyresponseid AS surrogate_survey_response_id,
            ngpvan_instance||'|'|| mastersurveyresponseid AS surrogate_master_survey_response_id

        FROM source

    )

SELECT * FROM renamed