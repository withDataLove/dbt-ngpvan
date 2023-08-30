WITH
    activists AS (SELECT * FROM {{ ref("stg_ngpvan_classic__contacts_activist_codes") }}),

    codes AS (SELECT * FROM {{ ref("stg_ngpvan_classic__activist_codes") }}),

    inputs AS (SELECT * FROM {{ ref("stg_ngpvan_classic__input_types") }}),

    contacts AS (SELECT * FROM {{ ref("stg_ngpvan_classic__contact_types") }})

SELECT
    activists.surrogate_contacts_activist_code_id,
    activists.contacts_activist_code_id,
    activists.ngpvan_database_mode,
    activists.van_id,
    surrogate_activist_code_id,
    activists.activist_code_id,
    codes.activist_code_type,
    codes.activist_code,
    codes.activist_code_description,
    activists.committee_id,
    activists.utc_created_at,
    activists.utc_modified_at,
    activists.input_type_id,
    inputs.input_type,
    activists.contact_type_id,
    contacts.contact_type,
    username,
    activists.canvassed_by_user_id,
    activists.campaign_id,
    activists.content_id,
    activists.contacts_contact_id,
    activists.utc_canvassed_at
FROM activists
LEFT JOIN codes USING (surrogate_activist_code_id)
LEFT JOIN inputs USING (surrogate_input_type_id)
LEFT JOIN contacts USING (surrogate_contact_type_id)
