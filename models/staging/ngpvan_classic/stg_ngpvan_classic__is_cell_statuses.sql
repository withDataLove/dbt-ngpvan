WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__is_cell_statuses') }}

    ),

    renamed AS (

        SELECT
            iscellstatusid AS cell_status_id,
            iscellstatusname AS cell_status,
            source_schema,
            source_table,

            -- additional columns
            ngpvan_instance||'|'||iscellstatusid AS surrogate_cell_status_id

        FROM source

    )

SELECT * FROM renamed
