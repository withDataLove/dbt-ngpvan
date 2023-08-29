WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__email_delta') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            contactsemailid AS contacts_email_id,
            vanid AS van_id,
            LOWER(TRIM(email)) AS email_address,
            committeeid AS committee_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datesuppressed) AS utc_suppressed_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            createdby AS created_by_user_id,
            emailsourcename AS email_source,
            emailtypeid AS email_type_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactsemailid AS surrogate_contacts_email_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id,
            ngpvan_instance||'|'||emailtypeid AS surrogate_email_type_id,
            CASE WHEN datesuppressed IS NOT NULL THEN TRUE ELSE FALSE END AS is_suppressed

        FROM source

    )

SELECT * FROM renamed
