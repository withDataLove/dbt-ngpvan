WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__online_form_types') }}
),
renamed AS (
    SELECT
        onlineformtypeid AS online_form_type_id,
        onlineformtypename AS online_form_type_name,

        -- additional columns
        ngpvan_instance||'|'||onlineformtypeid AS surrogate_online_form_type_id

    FROM source
)
SELECT * FROM renamed
