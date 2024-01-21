WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_contributions_codes') }}

    ),

    renamed AS (

        SELECT
            contactscontributionid AS contacts_contribution_id,
            codeid AS code_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            createdby AS created_by_user_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactscontributionid AS surrogate_contacts_contribution_id

        FROM source

    )

SELECT * FROM renamed
