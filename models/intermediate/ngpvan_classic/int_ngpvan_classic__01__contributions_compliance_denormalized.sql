WITH basic AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_basic') }}
),

advance AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_advanced') }}
)

select
    surrogate_contacts_contribution_id,
    surrogate_contacts_contribution_compliance_id,
    contacts_contribution_id,
    finance_program_id
from basic
