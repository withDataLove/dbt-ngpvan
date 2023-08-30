WITH events AS (SELECT * FROM {{ ref("stg_ngpvan_classic__events") }})

SELECT
    surrogate_event_id,
    event_id,
    utc_created_at,
    utc_modified_at,
    utc_suppressed_at,
    event_name,
    event_short_name,
    event_description,
    utc_event_start_at,
    utc_event_end_at,
    event_repetition_id,
    -- Event Type
    event_type_id,
    event_type,
    CASE WHEN event_type = 'BOC meeting' THEN TRUE ELSE FALSE END AS is_boc_meeting,
    CASE
        WHEN event_type = 'BOE/BOR meeting' THEN TRUE ELSE FALSE
    END AS is_boe_bor_meeting,
    CASE WHEN event_type = 'Canvass' THEN TRUE ELSE FALSE END AS is_canvass,
    CASE
        WHEN event_type = 'Community Event' THEN TRUE ELSE FALSE
    END AS is_community_event,
    CASE WHEN event_type = 'Data Entry' THEN TRUE ELSE FALSE END AS is_data_entry,
    CASE WHEN event_type = 'GOTV' THEN TRUE ELSE FALSE END AS is_gotv,
    CASE WHEN event_type = 'House Party' THEN TRUE ELSE FALSE END AS is_house_party,
    CASE WHEN event_type = '1:1s' THEN TRUE ELSE FALSE END AS is_one_on_one,
    CASE WHEN event_type = 'Phone Banks' THEN TRUE ELSE FALSE END AS is_phone_bank,
    CASE
        WHEN event_type = 'Political 1:1s' THEN TRUE ELSE FALSE
    END AS is_political_one_on_one,
    CASE WHEN event_type = 'Text Bank' THEN TRUE ELSE FALSE END AS is_text_bank,
    CASE WHEN event_type = 'Training' THEN TRUE ELSE FALSE END AS is_training,
    CASE
        WHEN event_type = 'Vol Recruitment' THEN TRUE ELSE FALSE
    END AS is_vol_recruitment,
    CASE
        WHEN event_type = 'Voter Protection' THEN TRUE ELSE FALSE
    END AS is_voter_protection,
    created_by_user_id,
    data_sharing_committee_id,
    committee_name,
    created_by_committee_id,
    created_by_committee_name,
    is_public_website_viewable

FROM events
