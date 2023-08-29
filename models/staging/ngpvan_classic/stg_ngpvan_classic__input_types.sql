WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__input_types') }}

    ),

    renamed AS (

        SELECT
            inputtypeid AS input_type_id,
            inputtypename AS input_type,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||inputtypeid AS surrogate_input_type_id

        FROM source

    )

SELECT * FROM renamed