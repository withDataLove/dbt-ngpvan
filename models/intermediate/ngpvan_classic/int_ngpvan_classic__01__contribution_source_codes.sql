WITH contribution_codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_codes') }}

),

codes AS (

    SELECT * FROM {{ ref('stg_ngpvan_classic__codes') }}
)

SELECT *
FROM contribution_codes
