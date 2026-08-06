INSERT INTO silver.stories (
    item_id, item_type, title, url, domain, author, score, descendants,
    is_dead, is_deleted, posted_at, captured_at, first_seen_at,
    version_count, dag_run_id
)
SELECT DISTINCT ON (b.item_id)
    b.item_id,
    b.payload->>'type'                                 AS item_type,
    b.payload->>'title'                                AS title,
    b.payload->>'url'                                  AS url,
    CASE
        WHEN b.payload->>'url' IS NOT NULL AND b.payload->>'url' != ''
            THEN regexp_replace(b.payload->>'url', '^(?:https?://)?(?:www\.)?([^/]+).*$', '\1')
        ELSE NULL
    END                                                 AS domain,
    b.payload->>'by'                                   AS author,
    (b.payload->>'score')::integer                     AS score,
    (b.payload->>'descendants')::integer                AS descendants,
    COALESCE((b.payload->>'dead')::boolean, false)      AS is_dead,
    COALESCE((b.payload->>'deleted')::boolean, false)   AS is_deleted,
    to_timestamp((b.payload->>'time')::bigint)          AS posted_at,
    b.captured_at,
    b.captured_at                                       AS first_seen_at,
    (SELECT COUNT(*) FROM bronze.item_versions v WHERE v.item_id = b.item_id) AS version_count,
    b.dag_run_id
FROM bronze.item_versions b
WHERE b.dag_run_id = %(dag_run_id)s
  AND b.payload->>'type' IN ('story', 'job')
  AND b.payload ? 'time'
ORDER BY b.item_id, b.captured_at DESC
ON CONFLICT (item_id) DO UPDATE SET
    item_type     = EXCLUDED.item_type,
    title         = EXCLUDED.title,
    url           = EXCLUDED.url,
    domain        = EXCLUDED.domain,
    author        = EXCLUDED.author,
    score         = EXCLUDED.score,
    descendants   = EXCLUDED.descendants,
    is_dead       = EXCLUDED.is_dead,
    is_deleted    = EXCLUDED.is_deleted,
    posted_at     = EXCLUDED.posted_at,
    captured_at   = EXCLUDED.captured_at,
    version_count = EXCLUDED.version_count,
    dag_run_id    = EXCLUDED.dag_run_id
WHERE EXCLUDED.captured_at > silver.stories.captured_at;
