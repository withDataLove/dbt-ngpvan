WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__address_delta') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            contactsaddressid AS contacts_address_id,
            vanid AS van_id,
            address,
            city,
            state,
            zip5,
            zip4,
            latitude,
            longitude,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datesuppressed) AS utc_suppressed_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,        
            createdby AS created_by,
            sourcename AS source,
            addresstype AS address_type,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,        
            supersededbycontactsaddressid AS superseded_by_contacts_address_id,
            originalcontactsaddressid AS original_contacts_address_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactsaddressid AS surrogate_contacts_address_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id

        FROM source

    )

SELECT * FROM renamed