WITH contributions AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contributions_denormalized') }}
),

codes AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contribution_source_codes') }}
    WHERE is_current
),

compliance AS (
    SELECT *
    FROM {{ ref('int_ngpvan_classic__01__contributions_compliance_denormalized') }}
)

SELECT surrogate_contacts_contribution_id,
       contributions.contacts_contribution_id,
       contributions.surrogate_van_id,
       contributions.van_id,
       contributions.contribution_amount,
       contributions.utc_contribution_received_at,
       contributions.utc_contribution_created_at,
       contributions.contribution_status,
       contributions.financial_program_id,
       contributions.financial_program,
       contributions.direct_marketing_code,
       contributions.utc_contribution_record_modified_at,
       contributions.online_reference_number,
       codes.source_code,
       codes.source_code_description,
       codes.source_code_path,
       compliance.contribution_type,
       compliance.is_memo

  FROM contributions
  LEFT JOIN codes USING (surrogate_contacts_contribution_id)
  LEFT JOIN compliance USING (surrogate_contacts_contribution_id)





