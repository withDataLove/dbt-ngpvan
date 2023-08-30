WITH statuses AS (SELECT * FROM {{ ref("stg_ngpvan_classic__event_signups_statuses") }})

SELECT
    surrogate_event_signups_event_status_id,
    event_signups_event_status_id,
    surrogate_event_signup_id,
    event_signup_id,
    event_status_id,
    event_signup_status,
    created_by_user_id,
    created_by_username,
    utc_created_at,
    contacts_contact_id,
    utc_modified_at,
    source_schema,
    source_table,
    ngpvan_instance,

    -- additional columns
    CASE WHEN event_signup_status = 'Cancelled' THEN TRUE ELSE FALSE END AS is_cancelled,
    CASE WHEN event_signup_status = 'Confirmed' THEN TRUE ELSE FALSE END AS is_confirmed,
    CASE
        WHEN event_signup_status = 'Conf Twice' THEN TRUE ELSE FALSE
    END AS is_confirmed_twice,
    CASE WHEN event_signup_status = 'Completed' THEN TRUE ELSE FALSE END AS is_completed,
    CASE WHEN event_signup_status = 'Declined' THEN TRUE ELSE FALSE END AS is_declined,
    CASE WHEN event_signup_status = 'Invited' THEN TRUE ELSE FALSE END AS is_invited,
    CASE WHEN event_signup_status = 'Left Msg' THEN TRUE ELSE FALSE END AS is_left_message,
    CASE WHEN event_signup_status = 'No Show' THEN TRUE ELSE FALSE END AS is_no_show,
    CASE WHEN event_signup_status = 'Scheduled' THEN TRUE ELSE FALSE END AS is_scheduled,
    CASE
        WHEN event_signup_status = 'Sched-Web' THEN TRUE ELSE FALSE
    END AS is_scheduled_web,
    CASE WHEN event_signup_status = 'Tentative' THEN TRUE ELSE FALSE END AS is_tentative,
    CASE WHEN event_signup_status = 'Texted' THEN TRUE ELSE FALSE END AS is_texted,
    ROW_NUMBER() OVER (
        PARTITION BY surrogate_event_signup_id
        ORDER BY surrogate_event_signups_event_status_id DESC
    ) AS most_recent_status

FROM statuses
