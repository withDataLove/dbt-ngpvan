{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

with submitted as (
    select * from {{ ref('stg_ngpvan_classic__contacts_online_forms_responses') }}
),

responses as (
    select * from {{ ref('int_ngpvan_classic__02__online_form_responses_enhanced') }}
)

select
    submitted.contacts_online_forms_response_id,
    submitted.contacts_online_form_id,
    responses.online_form_id,
    submitted.online_form_question_id,
    responses.online_form_question_name,
    responses.online_form_question_display_name,
    responses.online_form_question_type_name,
    responses.order_by,
    submitted.online_form_response_id,
    responses.online_form_response_name,
    responses.online_form_response_display_name,
    responses.is_other_response,
    submitted.response_value,
    submitted.other_response_value,
    submitted.surrogate_contacts_online_forms_response_id,
    submitted.surrogate_contacts_online_form_id,
    responses.surrogate_online_form_id,
    submitted.surrogate_online_form_question_id,
    surrogate_online_form_response_id
from submitted
left join responses using (surrogate_online_form_response_id)
order by contacts_online_form_id desc, responses.order_by, submitted.online_form_response_id
