INSERT INTO control.dq_results (run_id, check_name, passed, observed)

SELECT
    %(run_id)s,
    'silver_no_duplicate_item_id',
    COUNT(*) = COUNT(DISTINCT item_id),
    'total=' || COUNT(*) || ' distinct=' || COUNT(DISTINCT item_id)
FROM silver.stories

UNION ALL

SELECT
    %(run_id)s,
    'silver_no_invalid_values',
    COUNT(*) = 0,
    COUNT(*) || ' linhas com score<0 ou posted_at no futuro'
FROM silver.stories
WHERE score < 0 OR posted_at > NOW()

UNION ALL

SELECT
    %(run_id)s,
    'silver_count_not_shrinking',
    (SELECT COUNT(*) FROM silver.stories) >= COALESCE(
        (
            SELECT observed::int
            FROM control.dq_results
            WHERE check_name = 'silver_count_not_shrinking'
              AND run_id != %(run_id)s
            ORDER BY checked_at DESC
            LIMIT 1
        ),
        0
    ),
    (SELECT COUNT(*) FROM silver.stories)::text

UNION ALL

SELECT
    %(run_id)s,
    'silver_matches_bronze',
    NOT EXISTS (
        SELECT 1 FROM silver.stories s
        WHERE NOT EXISTS (SELECT 1 FROM bronze.item_versions b WHERE b.item_id = s.item_id)
    ),
    (
        SELECT COUNT(*) FROM silver.stories s
        WHERE NOT EXISTS (SELECT 1 FROM bronze.item_versions b WHERE b.item_id = s.item_id)
    )::text || ' linhas do Silver sem correspondência no Bronze'

UNION ALL

SELECT
    %(run_id)s,
    'gold_current_ranking_not_empty',
    (SELECT COUNT(*) FROM gold.current_ranking) > 0,
    (SELECT COUNT(*) FROM gold.current_ranking)::text || ' linhas em gold.current_ranking';
