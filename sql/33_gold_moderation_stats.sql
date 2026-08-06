TRUNCATE TABLE gold.moderation_stats;

WITH moderated AS (
    -- universo: tudo que hoje está dead ou deleted no Silver
    SELECT item_id, first_seen_at, is_dead, is_deleted
    FROM silver.stories
    WHERE is_dead OR is_deleted
),
versions AS (
    SELECT
        b.item_id,
        b.payload,
        b.captured_at,
        COALESCE((b.payload->>'dead')::boolean, false)
            OR COALESCE((b.payload->>'deleted')::boolean, false) AS is_moderated_version
    FROM bronze.item_versions b
    JOIN moderated m ON m.item_id = b.item_id
),
last_clean AS (
    -- última versão antes da moderação: onde title/score/by ainda existem
    SELECT DISTINCT ON (item_id)
        item_id, payload
    FROM versions
    WHERE NOT is_moderated_version
    ORDER BY item_id, captured_at DESC
),
first_flagged AS (
    -- primeira versão em que dead/deleted aparece: momento do flag
    SELECT DISTINCT ON (item_id)
        item_id, captured_at AS flagged_at
    FROM versions
    WHERE is_moderated_version
    ORDER BY item_id, captured_at ASC
)
INSERT INTO gold.moderation_stats (
    item_id, title, author, domain, final_state,
    first_seen_at, flagged_at, minutes_until_flagged, score_when_flagged
)
SELECT
    m.item_id,
    lc.payload->>'title' AS title,
    lc.payload->>'by'    AS author,
    CASE
        WHEN lc.payload->>'url' IS NOT NULL AND lc.payload->>'url' != ''
            THEN regexp_replace(lc.payload->>'url', '^(?:https?://)?(?:www\.)?([^/]+).*$', '\1')
        ELSE NULL
    END AS domain,
    CASE WHEN m.is_deleted THEN 'deleted' ELSE 'dead' END AS final_state,
    m.first_seen_at,
    ff.flagged_at,
    ROUND(EXTRACT(EPOCH FROM (ff.flagged_at - m.first_seen_at)) / 60.0, 2) AS minutes_until_flagged,
    (lc.payload->>'score')::integer AS score_when_flagged
FROM moderated m
JOIN first_flagged ff ON ff.item_id = m.item_id
LEFT JOIN last_clean lc ON lc.item_id = m.item_id;
