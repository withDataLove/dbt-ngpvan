WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__contacts') }}

    ),

    renamed AS (

        SELECT
            statecode AS state_code,
            vanid AS van_id,
            INITCAP(firstname) AS first_name,
            INITCAP(middlename) AS middle_name,
            INITCAP(lastname) AS last_name,
            suffix,
            streetno AS street_number,
            streetnohalf AS street_number_half,
            streetprefix AS street_prefix,
            streetname AS street_name,
            streettype AS street_type,
            streetsuffix AS street_suffix,
            apttype AS apartment_type,
            aptno AS apartment_number,
            city,
            state,
            zip5,
            zip4,
            vaddress AS full_address,
            CAST(badvaddress AS BOOLEAN) AS is_missing_address,
            {{ normalize_phone_number('phone') }} AS preferred_phone_number,
            dob AS date_of_birth,
            sex,
            cdname AS congressional_district,
            sdname AS state_senate_district,
            hdname AS state_house_district,
            precinctname AS precinct,
            countyname AS county,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datecreated) AS utc_created_at,
            committeeid AS committee_id,
            createdbycommitteeid AS created_by_committee_id,
            createdby AS created_by_user_id,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datemodified) AS utc_modified_at,
            CONVERT_TIMEZONE('America/New_York', 'UTC', datesuppressed) AS utc_suppressed_at,
            CAST(deceased AS BOOLEAN) AS is_deceased,
            employername AS employer,
            occupationname AS occupation,
            CAST(COALESCE(nocall, 0) AS BOOLEAN) AS do_not_call,
            CAST(COALESCE(noemail, 0) AS BOOLEAN) AS do_not_email,
            CAST(COALESCE(nomail, 0) AS BOOLEAN) AS do_not_mail,
            CAST(COALESCE(notext, 0) AS BOOLEAN) AS do_not_text,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            CASE WHEN datesuppressed IS NOT NULL THEN TRUE ELSE FALSE END AS is_suppressed,
            ngpvan_instance||'|'||vanid AS surrogate_van_id

        FROM source

    )

SELECT * FROM renamed
