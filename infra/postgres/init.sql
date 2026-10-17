-- GitHub Analytics serving database schema
-- Applied automatically by postgres container on first boot
-- v0.3.0: Removed retention_predictions and realtime_events tables (ML removed, Kafka deferred).
--         Updated ingestion_jobs status enum comment to include hdfs_write and spark_etl stages.

-- ──────────────────────────────────────────────────────────
-- Repositories
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS repositories (
    id              SERIAL PRIMARY KEY,
    owner           TEXT NOT NULL,
    name            TEXT NOT NULL,
    full_name       TEXT GENERATED ALWAYS AS (owner || '/' || name) STORED,
    stars           INT  DEFAULT 0,
    forks           INT  DEFAULT 0,
    language        TEXT,
    default_branch  TEXT DEFAULT 'main',
    head_commit_hash TEXT,
    is_private      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (owner, name)
);

-- ──────────────────────────────────────────────────────────
-- Contributors
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS contributors (
    id              SERIAL PRIMARY KEY,
    github_login    TEXT UNIQUE NOT NULL,
    email           TEXT,
    first_seen      DATE,
    last_seen       DATE,
    total_commits   BIGINT  DEFAULT 0,
    total_prs       INT     DEFAULT 0,
    total_reviews   INT     DEFAULT 0,
    total_issues    INT     DEFAULT 0
);

-- ──────────────────────────────────────────────────────────
-- Commit stats (pre-aggregated from Spark / Python pipeline)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS commit_stats (
    id              SERIAL PRIMARY KEY,
    repo_id         INT REFERENCES repositories(id),
    contributor_id  INT REFERENCES contributors(id),
    period          DATE   NOT NULL,          -- truncated to month (YYYY-MM-01)
    commit_count    INT    DEFAULT 0,
    insertions      BIGINT DEFAULT 0,
    deletions       BIGINT DEFAULT 0,
    files_changed   INT    DEFAULT 0,
    UNIQUE (repo_id, contributor_id, period)
);

-- ──────────────────────────────────────────────────────────
-- Commit type distribution
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS commit_types (
    id              SERIAL PRIMARY KEY,
    repo_id         INT REFERENCES repositories(id),
    period          DATE NOT NULL,
    commit_type     TEXT NOT NULL,   -- FEATURE | BUG_FIX | REFACTOR | DOCUMENTATION | PERFORMANCE | TEST | SECURITY | DEVOPS | OTHER
    count           INT  DEFAULT 0,
    UNIQUE (repo_id, period, commit_type)
);

-- ──────────────────────────────────────────────────────────
-- Full Commit History (indexed for lazy loading & search)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS commits (
    id              BIGSERIAL PRIMARY KEY,
    repo_id         INT REFERENCES repositories(id) ON DELETE CASCADE,
    hash            TEXT NOT NULL,
    author_name     TEXT NOT NULL,
    author_email    TEXT,
    committer_date  TIMESTAMPTZ,
    author_date     TIMESTAMPTZ,
    subject         TEXT,
    commit_type     TEXT DEFAULT 'OTHER',
    insertions      INT DEFAULT 0,
    deletions       INT DEFAULT 0,
    files_changed   INT DEFAULT 0,
    UNIQUE (repo_id, hash)
);

CREATE INDEX IF NOT EXISTS idx_commits_repo_date ON commits (repo_id, author_date DESC);
CREATE INDEX IF NOT EXISTS idx_commits_author ON commits (repo_id, author_name);
CREATE INDEX IF NOT EXISTS idx_commits_type ON commits (repo_id, commit_type);
CREATE INDEX IF NOT EXISTS idx_commits_hash ON commits (repo_id, hash);


-- ──────────────────────────────────────────────────────────
-- Graph analytics: collaboration edges
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS collaboration_edges (
    id              SERIAL PRIMARY KEY,
    repo_id         INT  REFERENCES repositories(id),
    dev_a           TEXT NOT NULL,
    dev_b           TEXT NOT NULL,
    weight          INT  DEFAULT 1,
    relationship    TEXT NOT NULL,  -- same_module | reviewed_pr | same_pr
    period          DATE,
    UNIQUE (repo_id, dev_a, dev_b, relationship, period)
);

-- ──────────────────────────────────────────────────────────
-- Graph metrics: PageRank & degree centrality per developer
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS graph_metrics (
    id                 SERIAL PRIMARY KEY,
    repo_id            INT REFERENCES repositories(id) ON DELETE CASCADE,
    dev                TEXT NOT NULL,
    pagerank           FLOAT DEFAULT 0.0,
    degree_centrality  INT DEFAULT 0,
    computed_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (repo_id, dev)
);

CREATE INDEX IF NOT EXISTS idx_graph_metrics_repo ON graph_metrics (repo_id, pagerank DESC);

-- ──────────────────────────────────────────────────────────
-- Project health scores (computed by the analytics pipeline)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS health_scores (
    id                      SERIAL PRIMARY KEY,
    repo_id                 INT REFERENCES repositories(id),
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    contributor_diversity   FLOAT,
    contributor_retention   FLOAT,
    collaboration           FLOAT,
    commit_consistency      FLOAT,
    bus_factor_safety       FLOAT,
    overall                 FLOAT,
    bus_factor_count        INT,
    top_contributor_pct     FLOAT
);

-- ──────────────────────────────────────────────────────────
-- Bus-factor per module
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bus_factor (
    id              SERIAL PRIMARY KEY,
    repo_id         INT REFERENCES repositories(id),
    module_path     TEXT NOT NULL,
    bus_factor      INT  NOT NULL,
    top_owner       TEXT,
    top_owner_pct   FLOAT,
    computed_at     DATE,
    UNIQUE (repo_id, module_path, computed_at)
);

-- ──────────────────────────────────────────────────────────
-- Ingestion Jobs (asynchronous repository processing tracker)
--
-- status lifecycle:
--   queued → cloning → extracting → hdfs_write → analytics → spark_etl → completed
--   (any stage may transition to: failed)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repo_url        TEXT NOT NULL,
    repo_owner      TEXT,
    repo_name       TEXT,
    branch          TEXT,
    status          TEXT NOT NULL DEFAULT 'queued',
    -- queued | cloning | extracting | hdfs_write | analytics | spark_etl | completed | failed
    progress_pct    INT DEFAULT 0,
    current_step    TEXT DEFAULT 'Queued in pipeline',
    total_commits   INT DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_created ON ingestion_jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_repo ON ingestion_jobs (repo_owner, repo_name);
CREATE INDEX IF NOT EXISTS idx_commit_stats_period ON commit_stats (repo_id, period DESC);
CREATE INDEX IF NOT EXISTS idx_collab_edges_weight ON collaboration_edges (repo_id, weight DESC);
CREATE INDEX IF NOT EXISTS idx_bus_factor_repo ON bus_factor (repo_id, top_owner_pct DESC);

-- ──────────────────────────────────────────────────────────
-- Streaming Tracked Repositories (dynamically managed from dashboard)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS streaming_repos (
    repo        TEXT PRIMARY KEY,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
