TRUNCATE TABLE gold.hourly_activity;

INSERT INTO gold.hourly_activity (
    hour_bucket, total_stories, avg_score, total_comments
)
SELECT
    date_trunc('hour', posted_at)  AS hour_bucket,
    COUNT(*)                       AS total_stories,
    ROUND(AVG(score), 2)           AS avg_score,
    SUM(descendants)               AS total_comments
FROM silver.stories
WHERE item_type = 'story'
  AND NOT is_dead
  AND NOT is_deleted
  AND posted_at >= NOW() - INTERVAL '7 days'
GROUP BY date_trunc('hour', posted_at)
ORDER BY hour_bucket;
