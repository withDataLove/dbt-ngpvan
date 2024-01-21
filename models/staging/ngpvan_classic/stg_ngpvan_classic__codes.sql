WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__codes') }}

    ),

    renamed AS (

        SELECT
            codeid AS code_id,
            codename AS code,
            committeeid AS committee_id,
            CAST(isactive AS BOOLEAN) AS is_active,
            parentcodeid AS parent_code_id,
            createdby AS created_by,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            staticfullname AS source_code_path,
            codetypeid AS code_type_id,
            codedescription AS code_description,
            contacttypeid AS contact_type_id,
            campaignid AS campaign_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||codeid AS surrogate_code_id,
            ngpvan_instance||'|'||parentcodeid AS surrogate_parent_code_id

        FROM source

    )

SELECT * FROM renamed
