WITH emails AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_emails') }}

),

email_types AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__email_types') }}
),

first AS (
    SELECT
        surrogate_van_id,
        MIN(contacts_email_id) AS contacts_email_id
    FROM emails
    WHERE NOT is_suppressed
    GROUP BY surrogate_van_id
),

most_recent AS (
    SELECT
        surrogate_van_id,
        MAX(contacts_email_id) AS contacts_email_id
    FROM emails
    WHERE NOT is_suppressed
    GROUP BY surrogate_van_id
),

most_recent_by_type AS (
    SELECT
        surrogate_van_id,
        email_type_id,
        MAX(contacts_email_id) AS contacts_email_id
    FROM emails
    WHERE NOT is_suppressed
    GROUP BY surrogate_van_id, email_type_id
),

multi_users AS (
    SELECT
        ngpvan_instance,
        committee_id,
        email_address,
        COUNT(DISTINCT surrogate_van_id) AS total_contacts
    FROM emails
    WHERE NOT is_suppressed
    GROUP BY ngpvan_instance,committee_id, email_address
    HAVING COUNT(DISTINCT surrogate_van_id) > 1
),

-- This CTE is identifying the most recent modified_at for a van_id across contacts_phones
most_recent_contact_modification AS (
    SELECT
        surrogate_van_id,
        MAX(utc_modified_at) AS utc_contact_modified_at
    FROM emails
    WHERE NOT is_suppressed
    GROUP BY surrogate_van_id
)

SELECT
    emails.surrogate_contacts_email_id,
    emails.contacts_email_id,
    emails.surrogate_van_id,
    emails.van_id,
    emails.email_address,
    emails.committee_id,
    emails.created_by_user_id,
    emails.email_type_id,
    email_types.email_type,
    emails.email_source,
    emails.utc_created_at,
    emails.utc_suppressed_at,
    emails.utc_modified_at,
    most_recent_contact_modification.utc_contact_modified_at,
    emails.is_suppressed,
    CASE WHEN first.contacts_email_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_first_contacts_email,
    CASE WHEN most_recent.contacts_email_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_most_recent_contacts_email,
    CASE WHEN most_recent_by_type.contacts_email_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_most_recent_contacts_email_by_type,
    CASE WHEN multi_users.email_address IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS has_multiple_users,
    COALESCE(multi_users.total_contacts, 1) AS total_contacts
FROM emails AS emails
LEFT JOIN email_types USING(surrogate_email_type_id)
LEFT JOIN first USING(surrogate_van_id, contacts_email_id)
LEFT JOIN most_recent USING(surrogate_van_id, contacts_email_id)
LEFT JOIN most_recent_by_type USING(surrogate_van_id, contacts_email_id)
LEFT JOIN multi_users USING(ngpvan_instance, committee_id, email_address)
LEFT JOIN most_recent_contact_modification USING(surrogate_van_id)
