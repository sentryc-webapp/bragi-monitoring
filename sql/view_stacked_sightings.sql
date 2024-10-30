DROP TABLE IF EXISTS temp_ml;

CREATE TEMP TABLE temp_ml AS
SELECT
    msp.sighting_id,
    msp.version
FROM
    stacked_ml_scored_products AS msp
WHERE
    msp.suggested = true
    AND msp.score > 40;

CREATE INDEX idx_temp_ml_sighting_id ON temp_ml (sighting_id);

WITH trigger_reason AS (
    SELECT
        s.id,
        s.ml_score IS NOT null
        AND (
            s.ml_score
            >= (p.permissions ->> 'scoreThresholdForTrademark')::float
        ) AS ml_score_triggered,
        p.permissions ->> 'trademarkScoreThreshold' IS NOT null
        AND s.trademark_score IS NOT null
        AND (
            s.trademark_score
            >= (p.permissions ->> 'trademarkScoreThreshold')::float
        ) AS logo_score_triggered
    FROM stacked_sightings AS s
    INNER JOIN producers AS p ON s.producer_id = p.id
),

last_model AS (
    SELECT
        s.producer_id,
        max(msp.version) AS last_version
    FROM ml_scored_products AS msp
    INNER JOIN sightings AS s ON msp.sighting_id = s.id AND msp.suggested = true
    WHERE msp.version LIKE '%EmbeddingModel%'
    GROUP BY s.producer_id
)

SELECT
    p.name,
    s.marketplace_id,
    s.created_at::date,
    msp.version,
    tr.ml_score_triggered,
    tr.logo_score_triggered,
    msp.version = lm.last_version AS last_model_version,
    count(*) AS total_count
FROM stacked_sightings AS s
INNER JOIN producers AS p ON s.producer_id = p.id
INNER JOIN last_model AS lm ON s.producer_id = lm.producer_id
INNER JOIN temp_ml AS msp ON s.id = msp.sighting_id
INNER JOIN trigger_reason AS tr ON s.id = tr.id
WHERE
    (tr.ml_score_triggered = true OR tr.logo_score_triggered = true)
    AND p.ml_score_enabled = true
    AND s.state = 'NEW'
GROUP BY
    p.name,
    s.created_at::date,
    s.marketplace_id,
    msp.version,
    lm.last_version,
    tr.ml_score_triggered,
    tr.logo_score_triggered
