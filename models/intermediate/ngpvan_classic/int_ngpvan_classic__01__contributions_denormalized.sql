WITH
contributions AS (SELECT * FROM {{ ref("stg_ngpvan_classic__contacts_contributions") }}),

users AS (SELECT * FROM {{ ref("stg_ngpvan_classic__users") }}),

payment_types AS (SELECT * FROM {{ ref("stg_ngpvan_classic__payment_types") }}),

statuses AS (SELECT * FROM {{ ref("stg_ngpvan_classic__contribution_statuses") }}),

programs AS (SELECT * FROM {{ ref("stg_ngpvan_classic__financial_programs") }}),

codes AS (SELECT * FROM {{ ref("stg_ngpvan_classic__codes") }})

SELECT
    contributions.surrogate_contacts_contribution_id,
    contributions.contacts_contribution_id,
    contributions.surrogate_van_id,
    contributions.van_id,
    contributions.contribution_amount,
    contributions.utc_contribution_received_at,
    contributions.utc_contribution_created_at,
    contributions.created_by_user_id,
    users.canvasser_name AS creator_name,
    users.email_address AS creator_email_address,
    contributions.committee_id,
    contributions.utc_contribution_modified_at,
    contributions.contribution_notes,
    contributions.payment_type_id,
    payment_types.payment_type,
    contributions.contribution_status_id,
    statuses.contribution_status,
    contributions.financial_program_id,
    programs.financial_program,
    contributions.direct_marketing_code,
    codes.source_code_path,
    contributions.utc_contribution_record_modified_at,
    contributions.online_reference_number

FROM contributions
LEFT JOIN users USING (surrogate_user_id)
LEFT JOIN payment_types USING (surrogate_payment_type_id)
LEFT JOIN statuses USING (surrogate_contribution_status_id)
LEFT JOIN programs USING (surrogate_financial_program_id)
LEFT JOIN codes USING (surrogate_code_id)
