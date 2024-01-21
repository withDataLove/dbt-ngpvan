WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_contributions_compliance_advanced') }}

    ),

    renamed AS (

        SELECT
            contactscontributionid AS contacts_contribution_id,
            financialprogramid AS financial_program_id,
            contributiontypeid AS contribution_type_id,
            CAST(CAST(ismemo AS INT) AS BOOLEAN) AS is_memo,
            note,
            itemizationflag AS itemization_flag,
            CAST(CAST(aggregatetowardslimit AS INT) AS BOOLEAN) AS is_aggregate_towards_limit,
            linenumberid AS line_number_id,
            CAST(CAST(advancedreportingoverrideon AS INT) AS BOOLEAN) AS is_advanced_reporting_override_on,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactscontributionid AS surrogate_contacts_contribution_id,
            ngpvan_instance||'|'||financialprogramid||'|'||contactscontributionid AS surrogate_contacts_contribution_compliance_id

        FROM source

    )

SELECT * FROM renamed
