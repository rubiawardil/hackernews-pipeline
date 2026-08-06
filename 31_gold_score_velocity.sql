TRUNCATE TABLE gold.score_velocity;

WITH item_versions AS (
    SELECT
        b.item_id,
        b.captured_at,
        (b.payload->>'score')::integer       AS score,
        (b.payload->>'descendants')::integer AS descendants
    FROM bronze.item_versions b
    WHERE b.payload->>'type' IN ('story', 'job')
      AND b.payload ? 'time'
),
numbered AS (
    SELECT
        item_id,
        captured_at,
        score,
        descendants,
        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY captured_at)     AS version_number,
        LAG(score) OVER (PARTITION BY item_id ORDER BY captured_at)       AS prev_score,
        LAG(captured_at) OVER (PARTITION BY item_id ORDER BY captured_at) AS prev_captured_at,
        COUNT(*) OVER (PARTITION BY item_id)                              AS total_versions
    FROM item_versions
)
INSERT INTO gold.score_velocity (
    item_id, title, version_number, captured_at, score, descendants,
    score_delta, minutes_since_previous
)
SELECT
    n.item_id,
    s.title,
    n.version_number,
    n.captured_at,
    n.score,
    n.descendants,
    n.score - n.prev_score AS score_delta,
    ROUND(EXTRACT(EPOCH FROM (n.captured_at - n.prev_captured_at)) / 60.0, 2) AS minutes_since_previous
FROM numbered n
JOIN silver.stories s ON s.item_id = n.item_id
WHERE n.total_versions > 1
ORDER BY n.item_id, n.version_number;
