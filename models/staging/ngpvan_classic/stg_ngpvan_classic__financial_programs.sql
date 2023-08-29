WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__financial_programs') }}

    ),

    renamed AS (

        SELECT
            financialprogramid AS financial_program_id,
            financialprogramname AS financial_program,
            CAST(istrackingtaxdeductibleamount AS BOOLEAN) AS is_tracking_tax_deductable_amount,
            CAST(isacceptinginternationalpayments AS BOOLEAN) AS is_accepting_international_payments,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||financialprogramid AS surrogate_financial_program_id

        FROM source

    )

SELECT * FROM renamed
