WITH filtered_sightings AS (
    SELECT *
    FROM sightings
    WHERE created_at > '2024-01-01'
),

trigger_reason AS (
    SELECT
        s.id,
        (
            s.ml_score IS NOT NULL
            AND s.ml_score
            >= (p.permissions ->> 'scoreThresholdForTrademark')::float
        ) AS ml_score_triggered,
        (
            p.permissions ->> 'trademarkScoreThreshold' IS NOT NULL
            AND s.trademark_score IS NOT NULL
            AND s.trademark_score
            >= (p.permissions ->> 'trademarkScoreThreshold')::float
        ) AS logo_score_triggered
    FROM filtered_sightings AS s
    INNER JOIN producers AS p ON s.producer_id = p.id
),

automatic_changes AS (
    SELECT
        sst.sighting_id AS id,
        SUM(CASE WHEN sst.new_state = 'PENDING' THEN 1 ELSE 0 END)
        > 0 AS pending_automated,
        SUM(CASE WHEN sst.new_state = 'NOT_RELEVANT' THEN 1 ELSE 0 END)
        > 0 AS not_relevant_automated
    FROM sightings_state_change_trace AS sst
    WHERE
        sst.created_by IS NULL
        AND sst.created_at > '2024-01-01'
    GROUP BY sst.sighting_id
),

max_scores AS (
    SELECT
        sighting_id,
        MAX(score) AS max_score
    FROM ml_scored_products
    WHERE
        suggested = TRUE
        AND version LIKE '%EmbeddingModel%'
    GROUP BY sighting_id
),

ranked_ml_scores AS (
    SELECT
        msp.sighting_id,
        msp.product_id,
        msp.version
    FROM ml_scored_products AS msp
    INNER JOIN max_scores AS ms ON
        msp.sighting_id = ms.sighting_id
        AND msp.score = ms.max_score
)

SELECT
    p.name AS producer_name,
    msp.version AS model_version,
    s.created_at::date,
    tr.ml_score_triggered,
    tr.logo_score_triggered,
    COALESCE(ac.pending_automated, FALSE) AS pending_automated,
    COALESCE(ac.not_relevant_automated, FALSE) AS not_relevant_automated,
    SUM((s.state = 'PENDING')::int) AS pending_count,
    SUM((s.state = 'NOT_RELEVANT')::int) AS not_relevant_count,
    SUM((s.state = 'BLOCKED')::int) AS blocked_count,
    SUM((s.state = 'IN_LITIGATION')::int) AS in_litigation_count,
    SUM((s.state = 'IGNORED')::int) AS ignored_count,
    SUM((s.state = 'PRE_SELECTED')::int) AS pre_selected_count,
    SUM((s.state = 'NEW')::int) AS new_count
FROM filtered_sightings AS s
INNER JOIN producers AS p ON s.producer_id = p.id
INNER JOIN ranked_ml_scores AS msp ON s.id = msp.sighting_id
INNER JOIN trigger_reason AS tr ON s.id = tr.id
LEFT JOIN automatic_changes AS ac ON s.id = ac.id
INNER JOIN products AS p1 ON s.product_id = p1.id
INNER JOIN products AS p2 ON msp.product_id = p2.id
GROUP BY
    p.name,
    msp.version,
    s.created_at::date,
    tr.ml_score_triggered,
    tr.logo_score_triggered,
    ac.pending_automated,
    ac.not_relevant_automated
