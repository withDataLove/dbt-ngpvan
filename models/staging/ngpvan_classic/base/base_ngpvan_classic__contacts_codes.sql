WITH base AS (
    {{
        union_all(
            source_schemas_variable='ngpvan_classic__schemas',
            source_tables_variable='contactscodes'
        )
    }}
)

SELECT *,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 2) AS source_schema,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 3) AS source_table,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 2) AS ngpvan_instance,
       CASE LOWER(RIGHT(SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 3), 4))
           WHEN '_myv' THEN 0
           WHEN '_myc' THEN 1 ELSE NULL END AS ngpvan_database_mode
  FROM base
