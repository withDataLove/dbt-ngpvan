WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__event_signups_statuses') }}

    ),

    renamed AS (

        SELECT
            eventsignupseventstatusid AS event_signups_event_status_id,
            eventsignupid AS event_signup_id,
            eventstatusid AS event_status_id,
            eventstatusname AS event_signup_status,
            createdby AS created_by_user_id,
            createdbyusername AS created_by_username,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            contactscontactid AS contacts_contact_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||eventsignupseventstatusid AS surrogate_event_signups_event_status_id,
            ngpvan_instance||'|'||eventsignupid AS surrogate_event_signup_id

        FROM source

    )

SELECT * FROM renamed
