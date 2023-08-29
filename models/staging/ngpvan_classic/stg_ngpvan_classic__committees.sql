WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__committees') }}

    ),

    renamed AS (

        SELECT
            committeeid AS committee_id,
            stateid AS state_id,
            committeename AS committee_name,
            committeeshortname AS committee_short_name,
            committeetypename AS committee_type,
            active AS status,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||committeeid AS surrogate_committee_id,
            CASE WHEN active = 1 THEN TRUE ELSE FALSE END AS is_active

        FROM source

    )

SELECT * FROM renamed
