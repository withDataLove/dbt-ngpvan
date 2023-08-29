WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__users') }}

    ),

    renamed AS (

        SELECT
            userid AS user_id,
            username,
            firstname AS first_name,
            lastname AS last_name,
            canvassername AS canvasser_name,
            address1 AS street_address,
            city,
            state,
            zip AS zip_code,
            LOWER(TRIM(email)) AS email_address,
            {{ normalize_phone_number('homephone') }} AS home_phone_number,
            {{ normalize_phone_number('cellphone') }} AS cell_phone_number,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||userid AS surrogate_user_id

        FROM source

    )

SELECT * FROM renamed