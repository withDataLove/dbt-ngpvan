WITH basic AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_basic') }}
),

advance AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contributions_compliance_advanced') }}
)

SELECT *
  FROM basic