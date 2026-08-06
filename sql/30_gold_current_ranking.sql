TRUNCATE TABLE gold.current_ranking;

WITH ranked AS (
    SELECT
        item_id,
        title,
        url,
        domain,
        author,
        score,
        descendants,
        posted_at,
        ROUND(EXTRACT(EPOCH FROM (NOW() - posted_at)) / 3600.0, 2) AS age_hours
    FROM silver.stories
    WHERE item_type = 'story'
      AND NOT is_dead
      AND NOT is_deleted
      AND score IS NOT NULL
)
INSERT INTO gold.current_ranking (
    rank_position, item_id, title, url, domain, author, score,
    descendants, age_hours, score_per_hour, posted_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY score DESC, posted_at DESC) AS rank_position,
    item_id, title, url, domain, author, score, descendants,
    age_hours,
    ROUND(score / NULLIF(age_hours, 0), 2) AS score_per_hour,
    posted_at
FROM ranked
ORDER BY score DESC, posted_at DESC
LIMIT 100;
