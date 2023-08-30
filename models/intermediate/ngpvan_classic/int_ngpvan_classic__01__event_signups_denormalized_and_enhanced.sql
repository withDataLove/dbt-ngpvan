WITH
signups AS (SELECT * FROM {{ ref("stg_ngpvan_classic__event_signups") }}),

input_types AS (SELECT * FROM {{ ref("stg_ngpvan_classic__input_types") }})

SELECT
    signups.surrogate_event_signup_id,
    signups.surrogate_event_id,
    signups.event_signup_id,
    signups.utc_modified_at AS utc_signup_modified_at,
    signups.utc_suppressed_at AS utc_signup_suppressed_at,
    signups.van_id,
    signups.event_id,
    signups.event_role,
    signups.event_shift_id,
    signups.utc_shift_begin_at,
    signups.utc_shift_end_at,
    signups.current_event_signups_status_id,
    input_types.input_type,
    signups.location_id,
    signups.canvassed_by_user_id,
    signups.created_by_user_id,
    signups.created_by_username,

    -- additional columns
    CASE WHEN event_role = 'Attendee' THEN TRUE ELSE FALSE END AS is_attendee,
    CASE
        WHEN event_role = 'Canvass Captain' THEN TRUE ELSE FALSE
    END AS is_canvass_captain,
    CASE WHEN event_role = 'Canvasser' THEN TRUE ELSE FALSE END AS is_canvasser,
    CASE WHEN event_role = 'Data Entry' THEN TRUE ELSE FALSE END AS is_data_entry,
    CASE WHEN event_role = 'Greeter' THEN TRUE ELSE FALSE END AS is_greeter,
    CASE WHEN event_role = 'Host' THEN TRUE ELSE FALSE END AS is_host,
    CASE WHEN event_role = 'Phone Captain' THEN TRUE ELSE FALSE END AS is_phone_captain,
    CASE WHEN event_role = 'Phonebanker' THEN TRUE ELSE FALSE END AS is_phonebanker,
    CASE
        WHEN event_role = 'Staging Location Director' THEN TRUE ELSE FALSE
    END AS is_staging_location_director,
    CASE WHEN event_role = 'Textbanker' THEN TRUE ELSE FALSE END AS is_textbanker,
    CASE WHEN event_role = 'Trainer' THEN TRUE ELSE FALSE END AS is_trainer,
    CASE WHEN event_role = 'Volunteer' THEN TRUE ELSE FALSE END AS is_volunteer

FROM signups
LEFT JOIN input_types USING (surrogate_input_type_id)
