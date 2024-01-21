WITH basic AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_basic') }}
),

advance AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_advanced') }}
)

select
    surrogate_contacts_contribution_id,
    surrogate_contacts_contribution_compliance_id,
    basic.contacts_contribution_id,
    basic.financial_program_id,
    advance.contribution_type,
    advance.is_memo
from basic
left join advance USING(surrogate_contacts_contribution_id, surrogate_contacts_contribution_compliance_id)
