TRUNCATE TABLE gold.top_domains;

INSERT INTO gold.top_domains (
    domain, total_stories, avg_score, median_score, max_score, total_comments
)
SELECT
    domain,
    COUNT(*)                                                             AS total_stories,
    ROUND(AVG(score), 2)                                                 AS avg_score,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score))::numeric, 2) AS median_score,
    MAX(score)                                                           AS max_score,
    SUM(descendants)                                                     AS total_comments
FROM silver.stories
WHERE item_type = 'story'
  AND NOT is_dead
  AND NOT is_deleted
  AND domain IS NOT NULL
GROUP BY domain
ORDER BY total_stories DESC, avg_score DESC
LIMIT 50;
