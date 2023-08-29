WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__phones_delta') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            contactsphoneid AS contacts_phone_id,
            vanid AS van_id,
            {{ normalize_phone_number('phone') }} AS phone_number,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datesuppressed) AS utc_suppressed_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,        
            createdby AS created_by_user_id,
            sourcename AS source,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,        
            phonetypeid AS phone_type_id,
            iscellstatusid AS cell_status_id,
            countrycode AS country_code,
            internationaldialingprefix AS international_dialing_prefix,
            createdbycommitteeid AS created_by_committee_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactsphoneid AS surrogate_contacts_phone_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id,
            ngpvan_instance||'|'||phonetypeid AS surrogate_phone_type_id,
            ngpvan_instance||'|'||iscellstatusid AS surrogate_cell_status_id,
            CASE WHEN countrycode = 'US' THEN TRUE ELSE FALSE END AS is_us_phone_number,
            CASE WHEN datesuppressed IS NOT NULL THEN TRUE ELSE FALSE END AS is_suppressed

        FROM source

    )

SELECT * FROM renamed
