WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_survey_responses') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            contactssurveyresponseid AS contacts_survey_response_id,
            vanid AS van_id,
            contactscontactid AS contacts_contact_id,
            surveyquestionid AS survey_question_id,
            surveyresponseid AS survey_response_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecanvassed) AS utc_canvassed_at,
            inputtypeid AS input_type_id,
            contacttypeid AS contact_type_id,
            committeeid AS committee_id,
            username,
            canvassedby AS canvassed_by_user_id,
            campaignid AS campaign_id,
            contentid AS content_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            teamid AS team_id,
            divisionid AS division_id,
            source_schema,
            source_table,
            ngpvan_instance,
            ngpvan_database_mode,

            -- additional columns
            ngpvan_instance||'|'||ngpvan_database_mode||'|'||contactssurveyresponseid AS surrogate_contacts_survey_response_id,
            ngpvan_instance||'|'||surveyresponseid AS surrogate_survey_response_id,
            ngpvan_instance||'|'||surveyquestionid AS surrogate_survey_question_id

        FROM source

    )

SELECT * FROM renamed