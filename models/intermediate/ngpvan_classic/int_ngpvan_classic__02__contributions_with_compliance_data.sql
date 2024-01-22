WITH contributions AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contributions_denormalized') }}
),

source_codes AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contribution_source_codes') }}
    WHERE is_current
),

compliance AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contributions_compliance_denormalized') }}
)

SELECT surrogate_contacts_contribution_id
FROM contributions
LEFT JOIN source_codes USING (surrogate_contacts_contribution_id)
LEFT JOIN compliance USING (surrogate_contacts_contribution_id)



