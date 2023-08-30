WITH deduped AS (
    select * from {{ ref('stg_ngpvan_classic__contacts_deduped') }}
),

contacts AS (
    select * from {{ ref('stg_ngpvan_classic__contacts') }}
)

select
    deduped.surrogate_contacts_deduped_id,
    deduped.surrogate_van_id,
    deduped.van_id,
    contacts.committee_id,
    deduped.surrogate_duplicate_van_id,
    deduped.duplicate_van_id,
    duplicates.committee_id AS duplicate_committee_id,
    contacts.is_suppressed,
    duplicates.is_suppressed AS is_duplicate_suppressed,
    deduped.utc_deduped_at,
    deduped.utc_modified_at,
    deduped.ngpvan_instance
from deduped
left join contacts
    on deduped.surrogate_van_id = contacts.surrogate_van_id
left join contacts as duplicates
    on deduped.surrogate_duplicate_van_id = duplicates.surrogate_van_id