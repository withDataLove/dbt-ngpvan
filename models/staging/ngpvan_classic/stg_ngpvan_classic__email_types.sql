WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__email_types') }}

    ),

    renamed AS (

        SELECT
            emailtypeid AS email_type_id,
            emailtypename AS email_type,
            source_schema,
            source_table,

            -- additional columns
            ngpvan_instance||'|'||emailtypeid AS surrogate_email_type_id


        FROM source

    )

SELECT * FROM renamed
