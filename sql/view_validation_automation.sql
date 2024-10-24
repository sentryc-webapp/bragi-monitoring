WITH new_state AS (
    SELECT
        sighting_id,
        created_at::date AS new_state_date
    FROM sightings_state_change_trace
    WHERE new_state = 'NEW'
),

automatic_changes AS (
    SELECT
        sst.sighting_id AS id,
        SUM(CASE WHEN sst.new_state = 'PENDING' THEN 1 ELSE 0 END)
        > 0 AS pending_automated,
        SUM(CASE WHEN sst.new_state = 'PRE_SELECTED' THEN 1 ELSE 0 END)
        > 0 AS pre_selected_automated,
        SUM(CASE WHEN sst.new_state = 'NOT_RELEVANT' THEN 1 ELSE 0 END)
        > 0 AS not_relevant_automated
    FROM sightings_state_change_trace AS sst
    WHERE (
        sst.created_by IS NULL
        AND sst.created_at > (
            SELECT MIN(sst2.created_at)
            FROM sightings_state_change_trace AS sst2
            WHERE sst2.created_by IS NOT NULL
        )
    )
    GROUP BY sst.sighting_id
),

ranked_ml_scores AS (
    SELECT
        msp.sighting_id,
        msp.product_id,
        msp.score,
        msp.version,
        ROW_NUMBER()
            OVER (PARTITION BY msp.sighting_id ORDER BY msp.score DESC)
        AS rn
    FROM ml_scored_products AS msp
    WHERE msp.suggested = TRUE
)

SELECT
    p.name AS producer_name,
    msp.version AS model_version,
    s.created_at::date,
    p1.title AS final_product,
    p2.title AS predicted_product,
    n.new_state_date::date,
    ac.pre_selected_automated,
    ac.pending_automated,
    SUM((s.state = 'PENDING')::int) AS pending_count,
    SUM((s.state = 'NOT_RELEVANT')::int) AS not_relevant_count,
    SUM((s.state = 'BLOCKED')::int) AS blocked_count,
    SUM((s.state = 'IN_LITIGATION')::int) AS in_litigation_count,
    SUM((s.state = 'IGNORED')::int) AS ignored_count,
    SUM((s.state = 'PRE_SELECTED')::int) AS pre_selected_count,
    SUM((s.state = 'NEW')::int) AS new_count
FROM sightings AS s
INNER JOIN producers AS p ON s.producer_id = p.id
INNER JOIN ranked_ml_scores AS msp ON s.id = msp.sighting_id
INNER JOIN new_state AS n ON s.id = n.sighting_id
INNER JOIN automatic_changes AS ac ON s.id = ac.id
INNER JOIN products AS p1 ON s.product_id = p1.id
INNER JOIN products AS p2 ON msp.product_id = p2.id
WHERE
    msp.version LIKE '%EmbeddingModel%'
    AND ac.pre_selected_automated = TRUE
GROUP BY
    p.name,
    msp.version,
    s.created_at::date,
    n.new_state_date::date,
    p1.title,
    p2.title,
    ac.pre_selected_automated, ac.pending_automated
