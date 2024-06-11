WITH base AS (
    {{
        union_all(
            source_schemas_variable='ngpvan_classic__schemas',
            default_source_table='onlineformquestiontypes',
            required_packages=['ngp']
        )
    }}
)

SELECT *,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 2) AS source_schema,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 3) AS source_table,
       SPLIT_PART(REPLACE(_dbt_source_relation, '"', ''), '.', 2) AS ngpvan_instance
  FROM base
