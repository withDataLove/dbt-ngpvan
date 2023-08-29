WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__phone_types') }}

    ),

    renamed AS (

        SELECT
            phonetypeid AS phone_type_id,
            phonetypename AS phone_type,
            source_schema,
            source_table,

            -- additional columns
            ngpvan_instance||'|'||phonetypeid AS surrogate_phone_type_id


        FROM source

    )

SELECT * FROM renamed
