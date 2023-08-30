WITH
responses AS (SELECT * FROM {{ ref("stg_ngpvan_classic__survey_responses") }}),

master_responses AS (SELECT * FROM {{ ref("stg_ngpvan_classic__master_survey_responses") }})

SELECT
    responses.surrogate_survey_response_id,
    responses.survey_response_id,
    responses.survey_response,
    responses.survey_question_id,
    democratic_points,
    republican_points,
    independent_points,
    master_responses.master_survey_response_id,
    master_responses.master_survey_question_id,
    master_responses.master_survey_response

FROM responses
LEFT JOIN master_responses USING (surrogate_master_survey_response_id)
