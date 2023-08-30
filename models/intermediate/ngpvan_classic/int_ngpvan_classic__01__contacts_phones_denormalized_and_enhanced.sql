WITH phones AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__contacts_phones') }}
),

phone_types AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__phone_types') }}
),
        
cell_statuses AS (
    SELECT * FROM {{ ref('stg_ngpvan_classic__is_cell_statuses') }}
),
        
most_recent_by_type AS (
    SELECT
        surrogate_van_id,
        phone_type_id,
        MAX(contacts_phone_id) AS contacts_phone_id
    FROM phones
    WHERE NOT is_suppressed
        AND is_us_phone_number IS TRUE
    GROUP BY surrogate_van_id, phone_type_id
),

most_recent AS (
    SELECT
        surrogate_van_id,
        MAX(contacts_phone_id) AS contacts_phone_id
    FROM phones
    WHERE NOT is_suppressed
        AND is_us_phone_number IS TRUE
    GROUP BY surrogate_van_id
),

most_recent_occurrence AS (
    SELECT
        surrogate_van_id,
        phone_number,
        MAX(contacts_phone_id) AS contacts_phone_id
    FROM phones
    WHERE NOT is_suppressed
        AND is_us_phone_number IS TRUE
    GROUP BY surrogate_van_id, phone_number
),

multi_users AS (
    SELECT
        ngpvan_instance,
        international_dialing_prefix,
        phone_number,
        COUNT(DISTINCT surrogate_van_id) AS total_contacts
    FROM phones
    WHERE NOT is_suppressed
        AND is_us_phone_number IS TRUE
    GROUP BY ngpvan_instance, international_dialing_prefix, phone_number
    HAVING COUNT(DISTINCT surrogate_van_id) > 1
),

-- This CTE is identifying the most recent modified_at for a van_id across contacts_phones
most_recent_contact_modification AS (
    SELECT
        surrogate_van_id,
        MAX(utc_modified_at) AS utc_contact_modified_at
    FROM phones
    WHERE NOT is_suppressed
        AND is_us_phone_number IS TRUE
    GROUP BY surrogate_van_id
)

SELECT 
    phones.surrogate_contacts_phone_id,
    phones.contacts_phone_id,
    phones.surrogate_van_id,
    phones.van_id,
    phones.phone_number,
    phones.phone_type_id,
    phone_types.phone_type,
    phones.cell_status_id,
    cell_statuses.cell_status,
    phones.created_by_user_id,
    phones.source,
    phones.international_dialing_prefix,
    phones.country_code,
    phones.is_us_phone_number,
    phones.utc_created_at,
    phones.utc_suppressed_at,
    phones.utc_modified_at,
    most_recent_contact_modification.utc_contact_modified_at,
    phones.is_suppressed,
    phones.created_by_committee_id,
    CASE WHEN most_recent_by_type.contacts_phone_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_most_recent_contacts_phone_by_type,
    CASE WHEN most_recent.contacts_phone_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_most_recent_contacts_phone,
    CASE WHEN most_recent_occurrence.contacts_phone_id IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS is_most_recent_contacts_phone_occurrence,
    CASE WHEN multi_users.phone_number IS NOT NULL
            THEN TRUE
        ELSE FALSE
    END AS has_multiple_users,
    COALESCE(multi_users.total_contacts, 1) AS total_contacts
FROM phones
LEFT JOIN phone_types USING(surrogate_phone_type_id)
LEFT JOIN cell_statuses USING(surrogate_cell_status_id)
LEFT JOIN most_recent_by_type USING(surrogate_van_id, contacts_phone_id)
LEFT JOIN most_recent USING(surrogate_van_id, contacts_phone_id)
LEFT JOIN multi_users USING(ngpvan_instance, international_dialing_prefix, phone_number)
LEFT JOIN most_recent_occurrence USING(surrogate_van_id, contacts_phone_id)
LEFT JOIN most_recent_contact_modification
    ON phones.surrogate_van_id = most_recent_contact_modification.surrogate_van_id
