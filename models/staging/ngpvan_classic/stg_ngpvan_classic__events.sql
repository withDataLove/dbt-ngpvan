WITH
    source AS (SELECT * FROM {{ ref("base_ngpvan_classic__events") }}),

    renamed AS (

        SELECT
            eventid AS event_id,
            eventcalendarid AS event_type_id,
            eventcalendarname AS event_type,
            eventname AS event_name,
            eventdescription AS event_description,
            CAST(
                CAST(dateoffsetbegin AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP
            ) AS utc_event_start_at,
            CAST(
                CAST(dateoffsetend AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP
            ) AS utc_event_end_at,
            createdby AS created_by_user_id,
            createdbyusername AS created_by_username,
            CONVERT_TIMEZONE(
                'America/New_York', 'UTC', CAST(datecreated AS TIMESTAMP)
            ) AS utc_created_at,
            datasharingcommitteeid AS data_sharing_committee_id,
            committeename AS committee_name,
            eventshortname AS event_short_name,
            eventrepetitionid AS event_repetition_id,
            CONVERT_TIMEZONE(
                'America/New_York', 'UTC', CAST(datesuppressed AS TIMESTAMP)
            ) AS utc_suppressed_at,
            createdbycommitteeid AS created_by_committee_id,
            createdbycommitteename AS created_by_committee_name,
            CAST(ispublicwebsiteviewable AS BOOLEAN) AS is_public_website_viewable,
            campaignid AS campaign_id,
            districtfieldvalue AS district_field_value,
            CONVERT_TIMEZONE(
                'America/New_York', 'UTC', CAST(datemodified AS TIMESTAMP)
            ) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance || '|' || eventid AS surrogate_event_id

        FROM source

    )

SELECT *
FROM renamed
