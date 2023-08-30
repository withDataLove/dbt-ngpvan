WITH
events AS (SELECT * FROM {{ ref("int_ngpvan_classic__01__events_enhanced") }}),

signups AS (
    SELECT * FROM {{ ref("int_ngpvan_classic__02__event_signups_with_current_statuses") }}
),

signup_stats AS (
    SELECT
        surrogate_event_id,
        event_id,
        COUNT(*) AS signups_count,
        COUNT(DISTINCT event_signup_id) AS unique_signups_count,
        SUM(CAST(is_cancelled AS INT)) AS cancelled_count,
        SUM(CAST(is_completed AS INT)) AS completed_count,
        SUM(CAST(is_confirmed AS INT)) AS confirmed_count,
        SUM(CAST(is_confirmed_twice AS INT)) AS confirmed_twice_count,
        SUM(CAST(is_declined AS INT)) AS declined_count,
        SUM(CAST(is_invited AS INT)) AS invited_count,
        SUM(CAST(is_left_message AS INT)) AS left_message_count,
        SUM(CAST(is_no_show AS INT)) AS no_show_count,
        SUM(CAST(is_scheduled AS INT)) AS scheduled_count,
        SUM(CAST(is_scheduled_web AS INT)) AS scheduled_web_count,
        SUM(CAST(is_tentative AS INT)) AS tentative_count,
        SUM(CAST(is_texted AS INT)) AS texted_count

    FROM signups
    GROUP BY surrogate_event_id, event_id
)

SELECT
    surrogate_event_id,
    events.event_id,
    events.utc_created_at,
    events.utc_modified_at,
    events.utc_suppressed_at,
    events.event_name,
    events.event_short_name,
    events.event_description,
    events.utc_event_start_at,
    events.utc_event_end_at,
    events.event_repetition_id,
    /* Event type details */
    events.event_type_id,
    events.event_type,
    events.is_boc_meeting,
    events.is_boe_bor_meeting,
    events.is_canvass,
    events.is_community_event,
    events.is_data_entry,
    events.is_gotv,
    events.is_house_party,
    events.is_one_on_one,
    events.is_phone_bank,
    events.is_political_one_on_one,
    events.is_text_bank,
    events.is_training,
    events.is_vol_recruitment,
    events.is_voter_protection,
    /* Event signups details */
    COALESCE(signup_stats.signups_count, 0) AS signups_count,
    COALESCE(signup_stats.unique_signups_count, 0) AS unique_signups_count,
    COALESCE(signup_stats.cancelled_count, 0) AS cancelled_count,
    COALESCE(signup_stats.completed_count, 0) AS completed_count,
    COALESCE(signup_stats.confirmed_count, 0) AS confirmed_count,
    COALESCE(signup_stats.confirmed_twice_count, 0) AS confirmed_twice_count,
    COALESCE(signup_stats.declined_count, 0) AS declined_count,
    COALESCE(signup_stats.invited_count, 0) AS invited_count,
    COALESCE(signup_stats.left_message_count, 0) AS left_message_count,
    COALESCE(signup_stats.no_show_count, 0) AS no_show_count,
    COALESCE(signup_stats.scheduled_count, 0) AS scheduled_count,
    COALESCE(signup_stats.scheduled_web_count, 0) AS scheduled_web_count,
    COALESCE(signup_stats.tentative_count, 0) AS tentative_count,
    COALESCE(signup_stats.texted_count, 0) AS texted_count,
    events.created_by_user_id,
    events.data_sharing_committee_id,
    events.committee_name,
    events.created_by_committee_id,
    events.created_by_committee_name,
    events.is_public_website_viewable
FROM events
LEFT JOIN signup_stats USING (surrogate_event_id)
