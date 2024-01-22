WITH contribution_codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_codes') }}

),

codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__codes') }}
),

most_recent AS (
    SELECT
        contacts_contribution_id,
        MAX(utc_created_at) AS utc_created_at,
        TRUE AS is_most_recent
    FROM contribution_codes
    GROUP BY 1
)

SELECT *,
       COALESCE(most_recent.is_most_recent, FALSE) AS is_current
FROM contribution_codes
LEFT JOIN most_recent USING (contacts_contribution_id, utc_created_at)
LEFT JOIN codes USING (code_id)
  
WHERE  contacts_contribution_id IN (9609269, 9669656)


