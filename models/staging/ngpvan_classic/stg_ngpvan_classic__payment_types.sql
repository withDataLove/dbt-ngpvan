WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__payment_types') }}

    ),

    renamed AS (

        SELECT
            paymenttypeid AS payment_type_id,
            paymenttypename AS payment_type,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||paymenttypeid AS surrogate_payment_type_id

        FROM source

    )

SELECT * FROM renamed