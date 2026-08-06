-- Roda automaticamente na primeira subida do container.
-- Para recriar do zero: docker compose down -v && docker compose up -d --build

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS control;


-- CONTROL

CREATE TABLE IF NOT EXISTS control.run_log (
    run_id            VARCHAR(250) PRIMARY KEY,
    started_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(50)  NOT NULL DEFAULT 'running',
    ids_discovered    INTEGER      DEFAULT 0,
    items_fetched     INTEGER      DEFAULT 0,
    bronze_inserted   INTEGER      DEFAULT 0,
    silver_upserted   INTEGER      DEFAULT 0,
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_log_started ON control.run_log (started_at DESC);

CREATE TABLE IF NOT EXISTS control.dq_results (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(250) NOT NULL,
    check_name   VARCHAR(200) NOT NULL,
    passed       BOOLEAN      NOT NULL,
    observed     TEXT,
    checked_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_results_run ON control.dq_results (run_id, checked_at DESC);


-- BRONZE
-- A API não tem campo de última modificação, então a mudança é detectada por
-- hash dos campos mutáveis do payload.
--
-- A PK (item_id, payload_hash) resolve duas coisas de uma vez: reexecutar a
-- mesma run não insere nada, porque o hash é o mesmo, e, quando o item muda de
-- verdade, a versão antiga fica preservada ao lado da nova. Esse histórico
-- não existe na origem e é o que alimenta o Gold.

CREATE TABLE IF NOT EXISTS bronze.item_versions (
    item_id       BIGINT       NOT NULL,
    payload_hash  CHAR(64)     NOT NULL,
    payload       JSONB        NOT NULL,
    source        VARCHAR(20)  NOT NULL,   -- new | top | updates
    captured_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    dag_run_id    VARCHAR(250) NOT NULL,
    PRIMARY KEY (item_id, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_bronze_item_captured
    ON bronze.item_versions (item_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_captured
    ON bronze.item_versions (captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_run
    ON bronze.item_versions (dag_run_id);


-- SILVER
-- Uma linha por item, sempre na versão mais recente. O upsert compara
-- captured_at para uma execução atrasada não sobrescrever dado mais novo.

CREATE TABLE IF NOT EXISTS silver.stories (
    item_id       BIGINT       PRIMARY KEY,
    item_type     VARCHAR(20)  NOT NULL,
    title         TEXT,
    url           TEXT,
    domain        VARCHAR(255),
    author        VARCHAR(100),
    score         INTEGER,
    descendants   INTEGER,
    is_dead       BOOLEAN      NOT NULL DEFAULT FALSE,
    is_deleted    BOOLEAN      NOT NULL DEFAULT FALSE,
    posted_at     TIMESTAMPTZ  NOT NULL,
    captured_at   TIMESTAMPTZ  NOT NULL,
    first_seen_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    version_count INTEGER      NOT NULL DEFAULT 1,
    dag_run_id    VARCHAR(250)
);

CREATE INDEX IF NOT EXISTS idx_silver_posted    ON silver.stories (posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_silver_domain    ON silver.stories (domain);
CREATE INDEX IF NOT EXISTS idx_silver_score     ON silver.stories (score DESC);
CREATE INDEX IF NOT EXISTS idx_silver_moderated ON silver.stories (is_dead, is_deleted);


-- GOLD
-- Reconstruído a cada execução.

CREATE TABLE IF NOT EXISTS gold.current_ranking (
    rank_position   INTEGER,
    item_id         BIGINT,
    title           TEXT,
    url             TEXT,
    domain          VARCHAR(255),
    author          VARCHAR(100),
    score           INTEGER,
    descendants     INTEGER,
    age_hours       NUMERIC(10, 2),
    score_per_hour  NUMERIC(10, 2),
    posted_at       TIMESTAMPTZ,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Montada a partir das versões do Bronze. É a prova de que o CDC funciona:
-- a API não devolve esse histórico.
CREATE TABLE IF NOT EXISTS gold.score_velocity (
    item_id                 BIGINT,
    title                   TEXT,
    version_number          INTEGER,
    captured_at             TIMESTAMPTZ,
    score                   INTEGER,
    descendants             INTEGER,
    score_delta             INTEGER,
    minutes_since_previous  NUMERIC(10, 2),
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_velocity_item
    ON gold.score_velocity (item_id, version_number);

CREATE TABLE IF NOT EXISTS gold.top_domains (
    domain          VARCHAR(255),
    total_stories   INTEGER,
    avg_score       NUMERIC(10, 2),
    median_score    NUMERIC(10, 2),
    max_score       INTEGER,
    total_comments  INTEGER,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Padrão temporal de publicação, populada por 34_gold_hourly_activity.sql
CREATE TABLE IF NOT EXISTS gold.hourly_activity (
    hour_bucket     TIMESTAMPTZ,
    total_stories   INTEGER,
    avg_score       NUMERIC(10, 2),
    total_comments  INTEGER,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Stories que a moderação marcou como dead/deleted, e quanto tempo levou
CREATE TABLE IF NOT EXISTS gold.moderation_stats (
    item_id                BIGINT,
    title                  TEXT,
    author                 VARCHAR(100),
    domain                 VARCHAR(255),
    final_state            VARCHAR(20),
    first_seen_at          TIMESTAMPTZ,
    flagged_at             TIMESTAMPTZ,
    minutes_until_flagged  NUMERIC(10, 2),
    score_when_flagged     INTEGER,
    refreshed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- VIEWS

CREATE OR REPLACE VIEW control.v_recent_runs AS
SELECT
    run_id,
    status,
    started_at,
    finished_at,
    ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS duration_seconds,
    ids_discovered,
    items_fetched,
    bronze_inserted,
    silver_upserted
FROM control.run_log
ORDER BY started_at DESC
LIMIT 20;

-- Atalho para a demo de idempotência: reexecutar a mesma run não pode mexer
-- nestes números, tirando o que a própria origem mudou nesse meio tempo
CREATE OR REPLACE VIEW control.v_layer_counts AS
SELECT 'bronze.item_versions' AS layer, COUNT(*) AS row_count FROM bronze.item_versions
UNION ALL
SELECT 'bronze.distinct_items',        COUNT(DISTINCT item_id) FROM bronze.item_versions
UNION ALL
SELECT 'silver.stories',               COUNT(*) FROM silver.stories
UNION ALL
SELECT 'gold.current_ranking',         COUNT(*) FROM gold.current_ranking
UNION ALL
SELECT 'gold.score_velocity',          COUNT(*) FROM gold.score_velocity
UNION ALL
SELECT 'gold.top_domains',             COUNT(*) FROM gold.top_domains
UNION ALL
SELECT 'gold.hourly_activity',         COUNT(*) FROM gold.hourly_activity
UNION ALL
SELECT 'gold.moderation_stats',        COUNT(*) FROM gold.moderation_stats;

CREATE OR REPLACE VIEW silver.v_most_revised AS
SELECT
    s.item_id,
    s.title,
    s.domain,
    s.score,
    s.descendants,
    COUNT(b.payload_hash) AS version_count,
    MIN(b.captured_at)    AS first_captured_at,
    MAX(b.captured_at)    AS last_captured_at
FROM silver.stories s
JOIN bronze.item_versions b ON b.item_id = s.item_id
GROUP BY s.item_id, s.title, s.domain, s.score, s.descendants
HAVING COUNT(b.payload_hash) > 1
ORDER BY COUNT(b.payload_hash) DESC
LIMIT 50;


-- PERMISSÕES

GRANT USAGE ON SCHEMA bronze, silver, gold, control TO hackernews;
GRANT ALL ON ALL TABLES    IN SCHEMA bronze, silver, gold, control TO hackernews;
GRANT ALL ON ALL SEQUENCES IN SCHEMA bronze, silver, gold, control TO hackernews;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold, control
    GRANT ALL ON TABLES TO hackernews;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold, control
    GRANT ALL ON SEQUENCES TO hackernews;


DO $$
BEGIN
    RAISE NOTICE 'Warehouse hackernewsdb pronto — schemas bronze, silver, gold, control';
END $$;
