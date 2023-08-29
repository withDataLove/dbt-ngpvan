WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_deduped') }}

    ),

    renamed AS (

        SELECT
            contactsdedupedid AS contacts_deduped_id,
            vanid AS van_id,
            dupvanid AS duplicate_van_id,
            dedupedby AS deduped_by_user_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datededuped) AS utc_deduped_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactsdedupedid AS surrogate_contacts_deduped_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id,
            ngpvan_instance||'|'||dupvanid AS surrogate_duplicate_van_id

        FROM source

    )

SELECT * FROM renamed
