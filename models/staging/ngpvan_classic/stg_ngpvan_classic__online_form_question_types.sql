{{
    config(
        tags=["ngpvan-online-forms"]
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('base_ngpvan_classic__online_form_question_types') }}
),
renamed AS (
    SELECT
        onlineformquestiontypeid AS online_form_question_type_id,
        onlineformquestiontypename AS online_form_question_type_name,

        -- additional columns
        ngpvan_instance||'|'||onlineformquestiontypeid AS surrogate_online_form_question_type_id

    FROM source
)
SELECT * FROM renamed
