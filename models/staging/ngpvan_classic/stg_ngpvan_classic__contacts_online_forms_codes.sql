WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__contacts_online_forms_codes') }}
),
renamed AS (
    SELECT
        contactsonlineformid AS contacts_online_form_id,
        codeid AS code_id,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_online_form_created_at,
        createdby AS created_by,
        CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_online_form_modified_at,

        -- additional columns
        ngpvan_instance||'|'||contactsonlineformid AS surrogate_contacts_online_form_id,
        ngpvan_instance||'|'||codeid AS surrogate_code_id
        
    FROM source
)
SELECT * FROM renamed
    