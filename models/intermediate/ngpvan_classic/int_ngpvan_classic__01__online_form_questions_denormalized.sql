{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

with questions as (
    select *
    from {{ ref('stg_ngpvan_classic__online_form_questions') }}
),

types as (
    select * from {{ ref('stg_ngpvan_classic__online_form_question_types') }}
)

select
    questions.online_form_question_id,
    questions.online_form_id,
    questions.online_form_question_name,
    questions.online_form_question_display_name,
    types.online_form_question_type_name,
    questions.activist_code_id,
    questions.survey_question_id,
    questions.order_by,
    questions.is_question_archived,
    questions.utc_online_form_question_created_at,
    questions.utc_online_form_question_modified_at,
    questions.utc_online_form_question_suppressed_at,
    questions.surrogate_online_form_question_id,
    questions.surrogate_online_form_question_type_id,
    questions.surrogate_online_form_id,
    questions.surrogate_activist_code_id,
    questions.surrogate_survey_question_id

from questions
inner join
    types
    on questions.surrogate_online_form_question_type_id = types.surrogate_online_form_question_type_id
