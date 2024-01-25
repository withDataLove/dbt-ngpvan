WITH contribution_codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_codes') }}

),

codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__codes') }}
),

most_recent AS (
    SELECT
        surrogate_contacts_contribution_id,
        MAX(utc_modified_at) AS utc_modified_at,
        TRUE AS is_most_recent
    FROM contribution_codes
    GROUP BY 1
)

SELECT surrogate_contacts_contribution_id,
       surrogate_code_id,
       contribution_codes.contacts_contribution_id,
       contribution_codes.code_id,
       contribution_codes.utc_created_at AS utc_source_code_created_at,
       contribution_codes.utc_modified_at AS utc_source_code_modified_at,
       codes.committee_id,
       codes.code AS source_code,
       codes.code_description AS source_code_description,
       codes.source_code_path,
       COALESCE(most_recent.is_most_recent, FALSE) AS is_current
FROM contribution_codes
LEFT JOIN most_recent USING (surrogate_contacts_contribution_id, utc_modified_at)
LEFT JOIN codes USING (surrogate_code_id)
