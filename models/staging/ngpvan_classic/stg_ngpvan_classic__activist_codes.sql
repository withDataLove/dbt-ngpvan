WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__activist_codes') }}

    ),

    renamed AS (

        SELECT
            activistcodeid AS activist_code_id,
            stateid AS state_id,
            activistcodetype AS activist_code_type,
            activistcodename AS activist_code,
            activistcodedescription AS activist_code_description,
            reportquestion AS report_question,
            dempoints AS democratic_points,
            reppoints AS republican_points,
            indpoints AS independent_points,
            committeecreatedby AS created_by_committee_id,
            actiontypeid AS action_type_id,
            campaignid AS campaign_id,

            -- additional columns
            ngpvan_instance||'|'||activistcodeid AS surrogate_activist_code_id

        FROM source

    )

SELECT * FROM renamed
