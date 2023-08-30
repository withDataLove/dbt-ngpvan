WITH questions AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__survey_questions') }}
),

master_questions AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__master_survey_questions') }}
)

SELECT
    questions.surrogate_survey_question_id,
    questions.ngpvan_instance,
    questions.survey_question_id,
    questions.survey_question_state_id,
    questions.survey_question_cycle,
    questions.survey_question_type,
    questions.survey_question,
    questions.survey_question_text,
    questions.master_survey_question_id,
    questions.created_by_committee_id,
    questions.survey_question_status,
    questions.has_master_survey_question,
    master_questions.master_survey_question_state_id,
    master_questions.master_survey_question_cycle,
    master_questions.master_survey_question_type,
    master_questions.master_survey_question,
    master_questions.master_survey_question_text,
    master_questions.action_type_id,
    master_questions.campaign_id

FROM questions
LEFT JOIN master_questions USING (surrogate_master_survey_question_id)