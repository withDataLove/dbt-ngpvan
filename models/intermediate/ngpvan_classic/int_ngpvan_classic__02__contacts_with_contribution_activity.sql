SELECT
    surrogate_van_id,
    van_id,
    MIN(utc_contribution_received_at) AS utc_first_contribution_received_at,
    MAX(utc_contribution_received_at) AS utc_most_recent_contribution_received_at
FROM {{ ref("int_ngpvan_classic__01__contributions_denormalized") }}
GROUP BY 1, 2
