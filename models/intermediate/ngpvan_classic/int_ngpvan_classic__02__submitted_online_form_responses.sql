with submitted_responses as (
    select * from dbt_mbrewsterffa_dev.stg_ngpvan_classic__contacts_online_forms_responses
),

responses as (
    select * from dbt_mbrewsterffa_dev.int_ngpvan_classic__02__online_form_responses_enhanced
),

submitted_forms as (
    select * from dbt_mbrewsterffa_dev.int_ngpvan_classic__02__submitted_online_forms
),

questions as (
    select * from dbt_mbrewsterffa_dev.int_ngpvan_classic__01__online_form_questions_denormalized
)

select
    -- submitted_responses.contacts_online_form_id,
    -- submitted_responses.online_form_question_id,
    -- submitted_responses.online_form_response_id,
    -- submitted_responses.response_value,
    -- submitted_responses.other_response_value,
    -- -- responses.*
    -- surrogate_contacts_online_form_id
    surrogate_contacts_online_forms_response_id, count(*) --, count(distinct surrogate_contacts_online_forms_response_id)
    
from submitted_responses
left join questions using (surrogate_online_form_question_id)
-- left join submitted_forms using (surrogate_contacts_online_form_id)
-- where submitted_forms.online_form_id = 504
-- left join responses using (surrogate_online_form_response_id)
-- where responses.online_form_id = 504
group by 1 having count(*) > 1