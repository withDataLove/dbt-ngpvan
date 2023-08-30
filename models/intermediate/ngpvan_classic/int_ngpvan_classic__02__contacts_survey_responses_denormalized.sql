WITH
contacts_responses AS (
    SELECT * FROM {{ ref("stg_ngpvan_classic__contacts_survey_responses") }}
),

responses AS (
    SELECT * FROM {{ ref("int_ngpvan_classic__01__survey_responses_denormalized") }}
),

questions AS (
    SELECT * FROM {{ ref("int_ngpvan_classic__01__survey_questions_denormalized") }}
)

SELECT
    contacts_responses.surrogate_contacts_survey_response_id,
    contacts_responses.contacts_survey_response_id,
    contacts_responses.van_id,
    contacts_responses.state_code,
    contacts_responses.contacts_contact_id,
    contacts_responses.survey_question_id,
    questions.survey_question_type,
    questions.survey_question,
    questions.survey_question_text,
    contacts_responses.survey_response_id,
    responses.survey_response,
    contacts_responses.utc_created_at,
    contacts_responses.utc_canvassed_at,
    contacts_responses.utc_modified_at,
    questions.survey_question_cycle,
    questions.survey_question_state_id,
    contacts_responses.input_type_id,
    contacts_responses.contact_type_id,
    contacts_responses.committee_id,
    contacts_responses.canvassed_by_user_id,
    contacts_responses.campaign_id,
    contacts_responses.content_id,
    contacts_responses.ngpvan_database_mode

FROM contacts_responses
LEFT JOIN responses USING (surrogate_survey_response_id)
LEFT JOIN questions USING (surrogate_survey_question_id)
