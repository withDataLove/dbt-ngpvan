WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contact_types') }}

    ),

    renamed AS (

        SELECT
            contacttypeid AS contact_type_id,
            contacttypename AS contact_type,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contacttypeid AS surrogate_contact_type_id


        FROM source

    )

SELECT * FROM renamed