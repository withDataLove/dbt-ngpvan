WITH
    source AS (

        SELECT * FROM {{ ref('base_ngpvan_classic__results') }}

    ),

    meta AS (

        SELECT DISTINCT
            _dbt_source_relation,
            source_schema,
            source_table,
            ngpvan_instance

        FROM source

    ),

    seeded AS (

        SELECT
            meta._dbt_source_relation,
            seed.resultid,
            seed.resultshortname,
            seed.resultdescription,
            meta.source_schema,
            meta.source_table,
            meta.ngpvan_instance
        FROM {{ ref('seed__ngpvan_classic__results') }} AS seed, meta
    ),

    unioned AS (
        SELECT * FROM source
        UNION
        SELECT * FROM seeded
    ),

    renamed AS (

        SELECT
            resultid AS result_id,
            resultshortname AS result,
            resultdescription AS result_description,
            source_schema,
            source_table,
            ngpvan_instance,

            -- additional columns
            ngpvan_instance||'|'||resultid AS surrogate_result_id

        FROM unioned

    )

SELECT * FROM renamed
