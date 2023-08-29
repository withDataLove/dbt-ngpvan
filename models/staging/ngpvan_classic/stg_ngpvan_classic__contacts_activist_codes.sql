WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_activist_codes') }}

    ),

    renamed AS (

        SELECT
            contactsactivistcodeid AS contacts_activist_code_id,
            vanid AS van_id,
            activistcodeid AS activist_code_id,
            committeeid AS committee_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            inputtypeid AS input_type_id,
            contacttypeid AS contact_type_id,
            username,
            canvassedby AS canvassed_by_user_id,
            campaignid AS campaign_id,
            contentid AS content_id,
            contactscontactid AS contacts_contact_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecanvassed) AS utc_canvassed_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,
            ngpvan_database_mode,

            -- additional columns
            ngpvan_instance||'|'||ngpvan_database_mode||'|'||contactsactivistcodeid AS surrogate_contacts_activist_code_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id,
            ngpvan_instance||'|'||activistcodeid AS surrogate_activist_code_id,
            ngpvan_instance||'|'||inputtypeid AS surrogate_input_type_id,
            ngpvan_instance||'|'||contacttypeid AS surrogate_contact_type_id

        FROM source

    )

SELECT * FROM renamed