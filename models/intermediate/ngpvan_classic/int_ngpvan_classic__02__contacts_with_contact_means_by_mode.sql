WITH contacts AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts') }}
),

emails AS (
    SELECT * FROM {{ ref('int_ngpvan_classic__01__contacts_emails_denormalized_and_enhanced') }}
),

phones AS (
    SELECT * FROM {{ ref('int_ngpvan_classic__01__contacts_phones_denormalized_and_enhanced') }}
),

preferred_phones AS (
    SELECT
        surrogate_van_id,
        phone_type,
        phone_number AS preferred_phone_number
    FROM phones
    WHERE is_most_recent_contacts_phone_occurrence IS TRUE
),

recent_phones AS (
    SELECT
        surrogate_van_id,
        phone_type,
        phone_number,
        utc_contact_modified_at
    FROM phones
    WHERE is_most_recent_contacts_phone IS TRUE
),

cell_phones AS (
    SELECT
        surrogate_van_id,
        phone_number,
        total_contacts
    FROM phones
    WHERE phone_type = 'Cell'
        AND is_most_recent_contacts_phone_by_type IS TRUE

),

home_phones AS (
    SELECT
        surrogate_van_id,
        phone_number,
        total_contacts
    FROM phones
    WHERE phone_type = 'Home'
        AND is_most_recent_contacts_phone_by_type IS TRUE

),

main_phones AS (
    SELECT
        surrogate_van_id,
        phone_number,
        total_contacts
    FROM phones
    WHERE phone_type = 'Main'
        AND is_most_recent_contacts_phone_by_type IS TRUE

),

personal_emails AS (
    SELECT
        surrogate_van_id,
        email_address,
        total_contacts
    FROM emails
    WHERE email_type = 'Personal'
        AND is_most_recent_contacts_email_by_type IS TRUE

),

work_emails AS (
    SELECT
        surrogate_van_id,
        email_address,
        total_contacts
    FROM emails
    WHERE email_type = 'Work'
        AND is_most_recent_contacts_email_by_type IS TRUE

),

first_emails AS (
    SELECT
        surrogate_van_id,
        email_address
    FROM emails
    WHERE is_first_contacts_email IS TRUE

),

recent_emails AS (
    SELECT
        surrogate_van_id,
        email_address,
        utc_contact_modified_at
    FROM emails
    WHERE is_most_recent_contacts_email IS TRUE

)

SELECT
    contacts.surrogate_van_id,
    contacts.van_id,

    -- Names
    contacts.first_name,
    contacts.middle_name,
    contacts.last_name,
    contacts.suffix,

    -- Address Info
    contacts.street_number,
    contacts.street_number_half,
    contacts.street_prefix,
    contacts.street_name,
    contacts.street_type,
    contacts.street_suffix,
    contacts.apartment_type,
    contacts.apartment_number,
    contacts.city,
    contacts.state,
    contacts.zip5,
    contacts.zip4,
    contacts.full_address,
    contacts.is_missing_address,
    -- is_complete_street_address is needed to help identify records with ful street addresses
    -- This is helpful for NGPVAN findOrCreate calls
    CASE WHEN contacts.street_number IS NOT NULL
            AND contacts.street_name IS NOT NULL
            AND contacts.full_address IS NOT NULL
            AND contacts.city IS NOT NULL
            AND contacts.state IS NOT NULL
            AND contacts.zip5 IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_complete_street_address,
    -- Phone Info
    preferred_phones.phone_type AS preferred_phone_type,
    preferred_phone_number,
    COALESCE(preferred_phone_number, recent_phones.phone_number) AS primary_phone_number,
    CASE WHEN preferred_phone_number IS NOT NULL AND preferred_phones.phone_type IS NOT NULL
            THEN preferred_phones.phone_type
        WHEN preferred_phone_number IS NOT NULL AND preferred_phones.phone_type IS NULL
            THEN 'Unknown'
        ELSE recent_phones.phone_type
    END AS primary_phone_type,
    cell_phones.phone_number AS cell_phone_number,
    cell_phones.total_contacts AS cell_total_contacts,
    main_phones.phone_number AS main_phone_number,
    main_phones.total_contacts AS main_total_contacts,
    home_phones.phone_number AS home_phone_number,
    home_phones.total_contacts AS home_total_contacts,

    -- Email Info
    first_emails.email_address AS first_email_address,
    recent_emails.email_address AS most_recent_email_address,
    work_emails.email_address AS work_email_address,
    personal_emails.email_address AS personal_email_address,
    personal_emails.total_contacts AS personal_email_total_contacts,

    -- Other PII
    contacts.date_of_birth,
    contacts.sex,
    contacts.employer,
    contacts.occupation,
    contacts.is_deceased,

    -- Voting Info
    contacts.congressional_district,
    contacts.state_senate_district,
    contacts.state_house_district,
    contacts.county,

    -- Contact Timestamps
    contacts.utc_created_at AS utc_contact_created_at,
    contacts.utc_suppressed_at AS utc_contact_suppressed_at,
    GREATEST(contacts.utc_modified_at, recent_phones.utc_contact_modified_at, recent_emails.utc_contact_modified_at) AS utc_contact_modified_at,
    contacts.is_suppressed,

    -- SII
    contacts.committee_id AS committee_id,
    contacts.created_by_committee_id AS created_by_committee_id,
    contacts.created_by_user_id,
    contacts.do_not_call,
    contacts.do_not_email,
    contacts.do_not_mail,
    contacts.do_not_text

FROM contacts
LEFT JOIN preferred_phones USING (surrogate_van_id, preferred_phone_number)
LEFT JOIN recent_phones USING (surrogate_van_id)
LEFT JOIN cell_phones USING (surrogate_van_id)
LEFT JOIN home_phones USING (surrogate_van_id)
LEFT JOIN main_phones USING (surrogate_van_id)
LEFT JOIN personal_emails USING (surrogate_van_id)
LEFT JOIN work_emails USING (surrogate_van_id)
LEFT JOIN first_emails USING (surrogate_van_id)
LEFT JOIN recent_emails USING (surrogate_van_id)
