WITH contacts_contacts AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_contacts') }}
),

results AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__results') }}
),

contact_types AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contact_types') }}
),

input_types AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__input_types') }}
)

SELECT
    contacts_contacts.surrogate_contacts_contact_id,
    contacts_contacts.contacts_contact_id,
    contacts_contacts.ngpvan_database_mode,
    contacts_contacts.van_id,
    surrogate_result_id,
    contacts_contacts.result_id,
    results.result,
    results.result_description,
    surrogate_contact_type_id,
    contacts_contacts.contact_type_id,
    contact_types.contact_type,
    surrogate_input_type_id,
    contacts_contacts.input_type_id,
    input_types.input_type,
    contacts_contacts.committee_id,
    contacts_contacts.utc_created_at,
    contacts_contacts.utc_canvassed_at,
    contacts_contacts.utc_modified_at,
    contacts_contacts.username,
    contacts_contacts.canvassed_by_user_id,
    contacts_contacts.content_id,
    contacts_contacts.created_by_user_id,
    -- Attempts Flags
    CASE WHEN contact_types.contact_type = 'Phone' THEN TRUE ELSE FALSE END AS is_phone_attempt,
    CASE
        WHEN contact_types.contact_type = 'SMS Text' THEN TRUE ELSE FALSE
    END AS is_sms_text_attempt,
    CASE WHEN contact_types.contact_type = 'Fax' THEN TRUE ELSE FALSE END AS is_fax_attempt,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' THEN TRUE ELSE FALSE
    END AS is_auto_dial_attempt,
    CASE
        WHEN contact_types.contact_type = 'Web Survey' THEN TRUE ELSE FALSE
    END AS is_web_survey_attempt,
    CASE
        WHEN contact_types.contact_type = 'Bulk Mail' THEN TRUE ELSE FALSE
    END AS is_bulk_mail_attempt,
    -- Results Flags
    -- All
    CASE
        WHEN results.result = 'Left Message' THEN TRUE ELSE FALSE
    END AS is_left_message,
    CASE WHEN results.result = 'Canvassed' THEN TRUE ELSE FALSE END AS is_canvassed,
    CASE WHEN results.result = 'Not Home' THEN TRUE ELSE FALSE END AS is_not_home,
    CASE
        WHEN results.result = 'Disconnected' THEN TRUE ELSE FALSE
    END AS is_disconnected,
    CASE
        WHEN results.result = 'Wrong Number' THEN TRUE ELSE FALSE
    END AS is_wrong_number,
    CASE WHEN results.result = 'Busy' THEN TRUE ELSE FALSE END AS is_busy,
    CASE WHEN results.result = 'Call Back' THEN TRUE ELSE FALSE END AS is_call_back,
    CASE WHEN results.result = 'Refused' THEN TRUE ELSE FALSE END AS is_refused,
    CASE
        WHEN results.result = 'Do Not Text' THEN TRUE ELSE FALSE
    END AS is_do_not_text,
    CASE WHEN results.result = 'Moved' THEN TRUE ELSE FALSE END AS is_moved,
    CASE
        WHEN results.result = 'Inaccessible' THEN TRUE ELSE FALSE
    END AS is_inaccessble,
    CASE WHEN results.result = 'Mailed' THEN TRUE ELSE FALSE END AS is_mailed,
    -- Phone
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Not Home'
            THEN TRUE
        ELSE FALSE
    END AS is_not_home_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Canvassed'
            THEN TRUE
        ELSE FALSE
    END AS is_canvassed_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Disconnected'
            THEN TRUE
        ELSE FALSE
    END AS is_disconnected_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Wrong Number'
            THEN TRUE
        ELSE FALSE
    END AS is_wrong_number_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Left Message'
            THEN TRUE
        ELSE FALSE
    END AS is_left_message_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Call Back'
            THEN TRUE
        ELSE FALSE
    END AS is_call_back_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Busy'
            THEN TRUE
        ELSE FALSE
    END AS is_busy_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Refused'
            THEN TRUE
        ELSE FALSE
    END AS is_refused_phone,
    CASE
        WHEN contact_types.contact_type = 'Phone' AND results.result = 'Moved'
            THEN TRUE
        ELSE FALSE
    END AS is_moved_phone,
    -- SMS Text
    CASE
        WHEN contact_types.contact_type = 'SMS Text' AND results.result = 'Left Message'
            THEN TRUE
        ELSE FALSE
    END AS is_left_message_sms_text,
    CASE
        WHEN contact_types.contact_type = 'SMS Text' AND results.result = 'Wrong Number'
            THEN TRUE
        ELSE FALSE
    END AS is_wrong_number_sms_text,
    CASE
        WHEN contact_types.contact_type = 'SMS Text' AND results.result = 'Do Not Text'
            THEN TRUE
        ELSE FALSE
    END AS is_do_not_text_sms_text,
    CASE
        WHEN contact_types.contact_type = 'SMS Text' AND results.result = 'Canvassed'
            THEN TRUE
        ELSE FALSE
    END AS is_canvassed_sms_text,
    CASE
        WHEN contact_types.contact_type = 'SMS Text' AND results.result = 'Moved'
            THEN TRUE
        ELSE FALSE
    END AS is_moved_sms_text,
    -- Fax
    CASE
        WHEN contact_types.contact_type = 'Fax' AND results.result = 'Canvassed'
            THEN TRUE
        ELSE FALSE
    END AS is_canvassed_fax,
    -- Auto Dial
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Not Home'
            THEN TRUE
        ELSE FALSE
    END AS is_not_home_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Canvassed'
            THEN TRUE
        ELSE FALSE
    END AS is_canvassed_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Disconnected'
            THEN TRUE
        ELSE FALSE
    END AS is_disconnected_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Wrong Number'
            THEN TRUE
        ELSE FALSE
    END AS is_wrong_number_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Left Message'
            THEN TRUE
        ELSE FALSE
    END AS is_left_message_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Busy'
            THEN TRUE
        ELSE FALSE
    END AS is_busy_auto_dial,
    CASE
        WHEN contact_types.contact_type = 'Auto Dial' AND results.result = 'Refused'
            THEN TRUE
        ELSE FALSE
    END AS is_refused_auto_dial,
    -- Web Survey
    CASE
        WHEN contact_types.contact_type = 'Web Survey' AND results.result = 'Canvassed'
            THEN TRUE
        ELSE FALSE
    END AS is_canvassed_web_survey,
    -- Bulk Mail
    CASE
        WHEN contact_types.contact_type = 'Bulk Mail' AND results.result = 'Mailed'
            THEN TRUE
        ELSE FALSE
    END AS is_mailed_bulk_mail

FROM contacts_contacts
LEFT JOIN results USING (surrogate_result_id)
LEFT JOIN contact_types USING (surrogate_contact_type_id)
LEFT JOIN input_types USING (surrogate_input_type_id)
