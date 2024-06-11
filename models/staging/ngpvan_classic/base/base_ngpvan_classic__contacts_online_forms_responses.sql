{{ dbt_utils.union_relations(
    relations=[
        ref('base_ngpvan_classic__contacts_online_forms_responses_multi'),
        ref('base_ngpvan_classic__contacts_online_forms_responses_boolean')
    ]
) }}
