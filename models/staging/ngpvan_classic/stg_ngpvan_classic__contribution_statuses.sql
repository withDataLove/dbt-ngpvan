WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contribution_statuses') }}

    ),

    renamed AS (

        SELECT
            contributionstatusid AS contribution_status_id,
            contributionstatusname AS contribution_status,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contributionstatusid AS surrogate_contribution_status_id

        FROM source

    )

SELECT * FROM renamed
