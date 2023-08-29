WITH
    source AS (
        SELECT * FROM {{ ref("base_ngpvan_classic__event_signups") }}
    ),

    renamed AS (

        SELECT
            eventsignupid AS event_signup_id,
            eventid AS event_id,
            vanid AS van_id,
            createdby AS created_by_user_id,
            createdbyuser AS created_by_username,
            eventroleid AS event_role_id,
            eventrolename AS event_role,
            eventshiftid AS event_shift_id,
            CAST(
                CAST(datetimeoffsetbegin AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP
            ) AS utc_shift_begin_at,

            CAST(
                CAST(datetimeoffsetend AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP
            ) AS utc_shift_end_at,

            locationid AS location_id,
            canvassedby AS canvassed_by_user_id,
            CONVERT_TIMEZONE(
                'America/New_York', 'UTC', CAST(datesuppressed AS TIMESTAMP)
            ) AS utc_suppressed_at,
            districtfieldvalue AS district_field_value,
            currenteventsignupseventstatusid AS current_event_signups_status_id,
            organizationid AS organization_id,
            supportergroupid AS supporter_group_id,
            CONVERT_TIMEZONE(
                'America/New_York', 'UTC', CAST(datemodified AS TIMESTAMP)
            ) AS utc_modified_at,
            inputtypeid AS input_type_id,
            zoomregistrationid AS zoom_registration_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance || '|' || eventsignupid AS surrogate_event_signup_id,
            ngpvan_instance || '|' || eventid AS surrogate_event_id,
            ngpvan_instance || '|' || input_type_id AS surrogate_input_type_id

        FROM source

    )

SELECT *
FROM renamed
