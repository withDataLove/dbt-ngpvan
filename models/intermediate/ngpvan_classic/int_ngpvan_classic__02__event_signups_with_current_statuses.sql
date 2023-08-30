WITH
signups AS (
    SELECT *
    FROM {{ ref("int_ngpvan_classic__01__event_signups_denormalized_and_enhanced") }}
),

statuses AS (
    SELECT *
    FROM {{ ref("int_ngpvan_classic__01__event_signups_statuses_enhanced") }}
    WHERE most_recent_status = 1
)

SELECT
    signups.surrogate_event_signup_id,
    signups.event_signup_id,
    signups.utc_signup_modified_at,
    signups.utc_signup_suppressed_at,
    signups.van_id,
    signups.surrogate_event_id,
    signups.event_id,
    signups.event_role,
    signups.is_attendee,
    signups.is_canvass_captain,
    signups.is_canvasser,
    signups.is_data_entry,
    signups.is_greeter,
    signups.is_host,
    signups.is_phone_captain,
    signups.is_phonebanker,
    signups.is_staging_location_director,
    signups.is_textbanker,
    signups.is_trainer,
    signups.is_volunteer,
    signups.event_shift_id,
    signups.utc_shift_begin_at,
    signups.utc_shift_end_at,
    signups.current_event_signups_status_id,
    statuses.event_signups_event_status_id,
    statuses.event_status_id AS event_status_id,
    statuses.event_signup_status,
    statuses.is_cancelled,
    statuses.is_confirmed,
    statuses.is_confirmed_twice,
    statuses.is_completed,
    statuses.is_declined,
    statuses.is_invited,
    statuses.is_left_message,
    statuses.is_no_show,
    statuses.is_scheduled,
    statuses.is_scheduled_web,
    statuses.is_tentative,
    statuses.is_texted,
    statuses.utc_created_at AS utc_status_created_at,
    statuses.utc_modified_at AS utc_status_modified_at,
    signups.input_type,
    signups.location_id,
    signups.canvassed_by_user_id,
    signups.created_by_user_id,
    signups.created_by_username

FROM signups
LEFT JOIN statuses USING (surrogate_event_signup_id)
