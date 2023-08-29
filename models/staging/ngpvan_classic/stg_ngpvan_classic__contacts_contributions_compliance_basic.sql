WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_contributions_compliance_basic') }}

    ),

    renamed AS (

        SELECT
            contactscontributionid AS contacts_contribution_id,
            financialprogramid AS financial_program_id,
            partnerlinkstatus AS partner_link_status,
            period,
            cycle,
            linkedvanid AS linked_van_id,
            linkedcontactscontributionid AS linked_contacts_contribution_id,
            linkedcontactsdisbursementid AS linked_contacts_disbursement_id,
            linkedcontactsdebtid AS linked_contacts_debt_id,
            CAST(CAST(isdebtrepayment AS INT) AS BOOLEAN) AS is_debt_repayment,
            loanduedate AS loan_due_date,
            loaninterestrate AS loan_interest_rate,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactscontributionid AS surrogate_contacts_contribution_id

        FROM source

    )

SELECT * FROM renamed