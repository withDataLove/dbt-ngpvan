WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_codes') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            contactscodeid AS contacts_code_id,
            vanid AS van_id,
            codeid AS code_id,
            committeeid AS committee_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            inputtypeid AS input_type_id,
            createdby AS created_by_user_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,
            ngpvan_database_mode,

            -- additional columns
            ngpvan_instance||'|'||ngpvan_database_mode||'|'||contactscodeid AS surrogate_contacts_code_id

        FROM source

    )

SELECT * FROM renamed
