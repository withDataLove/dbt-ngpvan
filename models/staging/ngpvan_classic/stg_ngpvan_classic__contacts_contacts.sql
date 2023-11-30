WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_contacts') }}

    ),

    renamed AS (

        SELECT
            contactscontactid AS contacts_contact_id,
            vanid AS van_id,
            resultid AS result_id,
            committeeid AS committee_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecanvassed) AS utc_canvassed_at,
            inputtypeid AS input_type_id,
            contacttypeid AS contact_type_id,
            username,
            canvassedby AS canvassed_by_user_id,
            campaignid AS campaign_id,
            contentid AS content_id,
            createdby AS created_by_user_id,
            personcommitteeid AS person_committee_id,
            contactsphoneid AS contacts_phone_id,
            teamid AS team_id,
            divisionid AS division_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,
            ngpvan_database_mode,

            -- additional columns
            ngpvan_instance||'|'||ngpvan_database_mode||'|'||contactscontactid AS surrogate_contacts_contact_id,
            ngpvan_instance||'|'||contacttypeid AS surrogate_contact_type_id,
            ngpvan_instance||'|'||inputtypeid AS surrogate_input_type_id,
            ngpvan_instance||'|'||resultid AS surrogate_result_id,
            ngpvan_instance||'|'||contactsphoneid AS surrogate_contacts_phone_id

        FROM source

    )

SELECT * FROM renamed
