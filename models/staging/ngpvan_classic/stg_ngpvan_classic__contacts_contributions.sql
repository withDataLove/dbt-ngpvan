WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts_contributions') }}

    ),

    renamed AS (

        SELECT
            contactscontributionid AS contacts_contribution_id,
            vanid AS van_id,
            CAST(amount AS DECIMAL(19,2)) AS contribution_amount,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datereceived) AS utc_contribution_received_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_contribution_created_at,
            createdby AS created_by_user_id,
            committeeid AS committee_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', contributiondatemodified) AS utc_contribution_modified_at,
            contributionmodifiedby AS contribution_modified_by,
            occupationname AS occupation_name,
            employername AS employer_name,
            notes AS contribution_notes,
            paymenttypeid AS payment_type_id,
            DATE(datethanked) AS contributor_thanked_date,
            contributionstatusid AS contribution_status_id,
            financialprogramid AS financial_program_id,
            contactsrecurringcontributionid AS contacts_recurring_contribution_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecanceled) AS utc_contribution_canceled_at,
            canceledby AS canceled_by,
            CONVERT_TIMEZONE('America/New_York', 'UTC', checkdate) AS utc_check_written_at,
            checknumber AS check_number,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datedeposited) AS utc_deposited_at,
            depositnumber AS deposit_number,
            directmarketingcode AS direct_marketing_code,
            financialvendorid AS financial_vendor_id,
            creditcardlast4digits AS credit_card_last_4_digits,
            DATE(creditcardexpirationdate) AS credit_card_expiration_date,
            contactsonlineformid AS contacts_online_form_id,
            gatewayaccountid AS gateway_account_id,
            bankaccountlast4digits AS bank_account_last_4_digits,
            bankaccounttypeid AS bank_account_type_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_contribution_record_modified_at,
            onlinereferencenumber AS online_reference_number,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datesettled) AS utc_contribution_settled_at,
            settlementbatch AS settlement_batch,
            CAST(fairmarketvalue AS DECIMAL(19,2)) AS fair_market_value,
            CAST(ispremiumgiftoptedout AS BOOLEAN) AS is_premium_gift_opted_out,
            processedcurrencyid AS processed_currency_id,
            CAST(processedamount AS DECIMAL(19,2)) AS processed_amount,
            settlementcurrencyid AS settlement_currency_id,
            CAST(settlementamount AS DECIMAL(19,2)) AS settlement_amount,
            CONVERT_TIMEZONE('America/New_York', 'UTC', dateposted) AS utc_contribution_posted_at,
            financialbatchid AS financial_batch_id,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||contactscontributionid AS surrogate_contacts_contribution_id,
            ngpvan_instance||'|'||vanid AS surrogate_van_id,
            ngpvan_instance||'|'||createdby AS surrogate_user_id,
            ngpvan_instance||'|'||paymenttypeid AS surrogate_payment_type_id,
            ngpvan_instance||'|'||contributionstatusid AS surrogate_contribution_status_id,
            ngpvan_instance||'|'||financialprogramid AS surrogate_financial_program_id,
            ngpvan_instance||'|'||codeid AS surrogate_code_id

        FROM source

    )

SELECT * FROM renamed
