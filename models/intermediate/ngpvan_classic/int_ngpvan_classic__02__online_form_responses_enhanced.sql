{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

with responses as (
    select *
    from {{ ref('stg_ngpvan_classic__online_form_responses') }}
),

questions as (
    select *
    from {{ ref('int_ngpvan_classic__01__online_form_questions_denormalized') }}
)

select
    responses.online_form_response_id,
    responses.online_form_question_id,
    online_form_response_name,
    online_form_response_display_name,
    is_other_response,
    responses.activist_code_id,
    responses.is_response_archived,
    questions.is_question_archived,
    is_note,
    responses.survey_question_id,
    survey_response_id,
    utc_online_form_response_created_at,
    utc_online_form_response_modified_at,
    utc_online_form_response_suppressed_at,
    surrogate_online_form_response_id,
    surrogate_online_form_question_id,
    responses.surrogate_survey_question_id,
    surrogate_survey_response_id,
    questions.online_form_id,
    questions.online_form_question_name,
    questions.online_form_question_display_name,
    questions.online_form_question_type_name,
    questions.order_by,
    questions.surrogate_online_form_id

from responses
left join
    questions
    on responses.surrogate_online_form_question_id = questions.surrogate_online_form_question_id
