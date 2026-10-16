"""
FastAPI serving layer — GitHub Contribution Analytics Lakehouse

Endpoints:
  POST /repos/ingest                      – async repo ingestion (unlimited depth, private token support)
  GET  /repos/jobs/{job_id}               – ingestion job status & real-time step progress
  GET  /repos/jobs                        – list recent ingestion jobs
  GET  /repos                             – list tracked repositories
  GET  /repos/{owner}/{name}/overview     – high-level analytics & metrics
  GET  /repos/{owner}/{name}/stats        – contributor stats (paginated, filterable)
  GET  /repos/{owner}/{name}/health       – project health score & breakdown
  GET  /repos/{owner}/{name}/busfactor    – bus-factor per module
  GET  /repos/{owner}/{name}/graph        – collaboration graph edges (D3 force graph)
  GET  /repos/{owner}/{name}/hotspots     – top code modules by churn / ownership concentration
  GET  /repos/{owner}/{name}/commit-types – commit type distribution
  POST /repos/{owner}/{name}/sync         – trigger incremental re-sync
  DELETE /repos/{owner}/{name}            – delete repository and cascade all analytics
  GET  /hdfs/status                       – HDFS namenode status and stored files
  GET  /spark/status                      – Spark cluster status from Spark master REST API
  GET  /health                            – service health check
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import asyncpg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi import BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from pipeline import (
    classify_subject,
    get_hdfs_client,
    parse_repo_url,
    run_ingestion_pipeline,
)

logger = logging.getLogger("api")

# ── SSE subscriber set ─────────────────────────────────────────────────────
# Each connected browser gets its own asyncio.Queue added here.
_sse_subscribers: set[asyncio.Queue] = set()

# ── Kafka consumer fan-out task ────────────────────────────────────────────
async def _kafka_fanout_task() -> None:
    """
    Bridges the kafka_consumer's asyncio.Queue → individual SSE subscriber queues.
    Runs as a background task so it doesn't block the event loop.
    """
    try:
        # Import consumer module from the mounted ingestion volume if available,
        # otherwise fall back to the installed package path.
        import sys, pathlib
        _consumer_dir = pathlib.Path("/ingestion/streaming")
        if _consumer_dir.exists() and str(_consumer_dir) not in sys.path:
            sys.path.insert(0, str(_consumer_dir))

        from kafka_consumer import consume_kafka_events, get_live_queue  # type: ignore
    except ImportError:
        logger.warning("kafka_consumer not found — live streaming disabled")
        return

    live_q      = get_live_queue()
    stop_event  = asyncio.Event()
    consumer_t  = asyncio.create_task(consume_kafka_events(stop_event))
    logger.info("Kafka consumer background task started")

    try:
        while True:
            event = await live_q.get()
            # Fan-out to all connected SSE subscribers
            dead: set[asyncio.Queue] = set()
            for sub_q in _sse_subscribers:
                try:
                    sub_q.put_nowait(event)
                except asyncio.QueueFull:
                    dead.add(sub_q)   # slow/disconnected client
            _sse_subscribers.difference_update(dead)
    except asyncio.CancelledError:
        stop_event.set()
        consumer_t.cancel()
        logger.info("Kafka fan-out task cancelled")


# ── App ────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    """Start Kafka consumer fan-out on startup; clean up on shutdown."""
    p = await pool()           # warm up DB pool
    try:
        await p.execute(
            """
            CREATE TABLE IF NOT EXISTS streaming_repos (
                repo TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        count = await p.fetchval("SELECT COUNT(*) FROM streaming_repos")
        if count == 0:
            existing = await p.fetch("SELECT full_name FROM repositories ORDER BY synced_at DESC LIMIT 3")
            if existing:
                for row in existing:
                    await p.execute("INSERT INTO streaming_repos (repo) VALUES ($1) ON CONFLICT DO NOTHING", row["full_name"])
            else:
                await p.execute("INSERT INTO streaming_repos (repo) VALUES ('torvalds/linux') ON CONFLICT DO NOTHING")
    except Exception as exc:
        logger.warning(f"Failed to initialize streaming_repos table: {exc}")

    fanout = asyncio.create_task(_kafka_fanout_task())
    try:
        yield
    finally:
        fanout.cancel()
        try:
            await fanout
        except asyncio.CancelledError:
            pass
        if _pool:
            await _pool.close()


app = FastAPI(
    title="GitHub Analytics Lakehouse API",
    description=(
        "Serves pre-computed analytics from the HDFS + Spark + Iceberg lakehouse "
        "and manages asynchronous repository ingestion."
    ),
    version="0.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB pool ────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://analytics:analytics@postgres:5432/github_analytics",
)

_pool: asyncpg.Pool | None = None


async def pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


# ── Health ─────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.3.0"}


# ── Request Models ─────────────────────────────────────────────────────────
class IngestRequest(BaseModel):
    repo_url: str = Field(
        ...,
        description="GitHub URL (e.g. https://github.com/owner/repo) or owner/repo shorthand",
    )
    access_token: str | None = Field(
        None,
        description="GitHub Personal Access Token for private repositories",
    )
    branch: str | None = Field(
        None,
        description="Target branch (defaults to remote default branch, e.g. main/master)",
    )


# ── Ingestion Job Endpoints ────────────────────────────────────────────────
@app.post("/repos/ingest")
async def trigger_ingestion(req: IngestRequest, background_tasks: BackgroundTasks):
    """
    Submits a repository for ingestion:
      - Zero depth limits (full commit history)
      - Private repositories supported with personal access tokens
      - Asynchronous processing with real-time status tracking
      - Triggers Spark CommitETL and GraphAnalytics after HDFS upload
    """
    try:
        owner, name = parse_repo_url(req.repo_url)
    except ValueError as e:
        raise HTTPException(400, str(e))

    job_id = uuid.uuid4()
    p = await pool()

    await p.execute(
        """
        INSERT INTO ingestion_jobs (id, repo_url, repo_owner, repo_name, branch, status, progress_pct, current_step)
        VALUES ($1, $2, $3, $4, $5, 'queued', 0, 'Queued in ingestion pipeline')
        """,
        job_id,
        req.repo_url,
        owner,
        name,
        req.branch or "main",
    )

    background_tasks.add_task(
        run_ingestion_pipeline,
        str(job_id),
        req.repo_url,
        req.access_token,
        req.branch,
    )

    return {
        "job_id": str(job_id),
        "owner": owner,
        "name": name,
        "status": "queued",
        "message": (
            f"Ingestion queued for {owner}/{name}. "
            f"Poll /repos/jobs/{job_id} for progress. "
            f"Spark jobs will run automatically after HDFS upload."
        ),
    }


@app.get("/repos/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Fetch status, progress percentage, commit counts, and error details of an ingestion job."""
    p = await pool()
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(400, "Invalid UUID format for job_id")

    row = await p.fetchrow("SELECT * FROM ingestion_jobs WHERE id = $1", job_uuid)
    if not row:
        raise HTTPException(404, f"Ingestion job '{job_id}' not found")

    return dict(row)


@app.get("/repos/jobs")
async def list_jobs(limit: int = Query(30, le=100)):
    """List recent repository ingestion jobs."""
    p = await pool()
    rows = await p.fetch(
        """
        SELECT id, repo_url, repo_owner, repo_name, branch, status,
               progress_pct, current_step, total_commits, error_message,
               started_at, completed_at, created_at
        FROM ingestion_jobs
        ORDER BY created_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


# ── Repositories ───────────────────────────────────────────────────────────
@app.get("/repos")
async def list_repos():
    """List all tracked repositories with synchronization metadata."""
    p = await pool()
    rows = await p.fetch(
        """
        SELECT id, owner, name, full_name, stars, forks, language,
               default_branch, head_commit_hash, is_private, synced_at
        FROM repositories
        ORDER BY synced_at DESC
        """
    )
    return [dict(r) for r in rows]


@app.get("/repos/{owner}/{name}/overview")
async def repo_overview(owner: str, name: str):
    """Returns high-level analytics, health scores, and metrics for a repository."""
    p = await pool()
    repo = await p.fetchrow(
        """
        SELECT id, owner, name, full_name, stars, forks, language,
               default_branch, head_commit_hash, is_private, synced_at
        FROM repositories
        WHERE owner=$1 AND name=$2
        """,
        owner,
        name,
    )
    if not repo:
        raise HTTPException(404, f"Repository {owner}/{name} not found")

    repo_id = repo["id"]

    health_row = await p.fetchrow(
        """
        SELECT contributor_diversity, contributor_retention, collaboration,
               commit_consistency, bus_factor_safety, overall,
               bus_factor_count, top_contributor_pct, computed_at
        FROM health_scores
        WHERE repo_id = $1
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        repo_id,
    )

    stats_agg = await p.fetchrow(
        """
        SELECT COUNT(DISTINCT contributor_id) AS total_contributors,
               COALESCE(SUM(commit_count), 0) AS total_commits,
               COALESCE(SUM(insertions), 0)   AS total_insertions,
               COALESCE(SUM(deletions), 0)    AS total_deletions,
               COALESCE(SUM(files_changed), 0)AS total_files_changed,
               MIN(period)                    AS first_commit_period,
               MAX(period)                    AS last_commit_period
        FROM commit_stats
        WHERE repo_id = $1
        """,
        repo_id,
    )

    top_modules = await p.fetch(
        """
        SELECT module_path, bus_factor, top_owner, top_owner_pct
        FROM bus_factor
        WHERE repo_id = $1
        ORDER BY top_owner_pct DESC
        LIMIT 5
        """,
        repo_id,
    )

    return {
        "repository": dict(repo),
        "health": dict(health_row) if health_row else None,
        "metrics": dict(stats_agg) if stats_agg else {},
        "critical_modules": [dict(m) for m in top_modules],
    }


@app.get("/repos/{owner}/{name}/hotspots")
async def repo_hotspots(owner: str, name: str, limit: int = Query(20, le=50)):
    """Returns modules with high churn or high ownership concentration."""
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, f"Repository {owner}/{name} not found")

    rows = await p.fetch(
        """
        SELECT module_path, bus_factor, top_owner, top_owner_pct, computed_at
        FROM bus_factor
        WHERE repo_id = $1
        ORDER BY top_owner_pct DESC
        LIMIT $2
        """,
        repo["id"],
        limit,
    )
    return [dict(r) for r in rows]


@app.post("/repos/{owner}/{name}/sync")
async def resync_repo(owner: str, name: str, background_tasks: BackgroundTasks):
    """Triggers a re-synchronization of an existing tracked repository."""
    p = await pool()
    repo = await p.fetchrow(
        "SELECT default_branch FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, f"Repository {owner}/{name} not found")

    repo_url = f"https://github.com/{owner}/{name}"
    branch   = repo["default_branch"] or "main"
    job_id   = uuid.uuid4()

    await p.execute(
        """
        INSERT INTO ingestion_jobs (id, repo_url, repo_owner, repo_name, branch, status, progress_pct, current_step)
        VALUES ($1, $2, $3, $4, $5, 'queued', 0, 'Re-sync queued')
        """,
        job_id,
        repo_url,
        owner,
        name,
        branch,
    )

    background_tasks.add_task(
        run_ingestion_pipeline,
        str(job_id),
        repo_url,
        None,
        branch,
    )

    return {
        "job_id": str(job_id),
        "status": "queued",
        "message": f"Re-sync queued for {owner}/{name}",
    }


@app.delete("/repos/{owner}/{name}")
async def delete_repo(owner: str, name: str):
    """Deletes a repository and cleans up all associated analytics tables."""
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, f"Repository {owner}/{name} not found")

    repo_id = repo["id"]
    async with p.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM commits WHERE repo_id = $1", repo_id)
            await conn.execute("DELETE FROM commit_stats WHERE repo_id = $1", repo_id)
            await conn.execute("DELETE FROM commit_types WHERE repo_id = $1", repo_id)
            await conn.execute("DELETE FROM health_scores WHERE repo_id = $1", repo_id)
            await conn.execute("DELETE FROM bus_factor WHERE repo_id = $1", repo_id)
            await conn.execute("DELETE FROM collaboration_edges WHERE repo_id = $1", repo_id)
            await conn.execute(
                "DELETE FROM ingestion_jobs WHERE repo_owner = $1 AND repo_name = $2",
                owner,
                name,
            )
            await conn.execute("DELETE FROM repositories WHERE id = $1", repo_id)

    return {"status": "deleted", "repository": f"{owner}/{name}"}


# ── Contributor stats ──────────────────────────────────────────────────────
@app.get("/repos/{owner}/{name}/stats")
async def repo_stats(
    owner: str,
    name: str,
    limit: int = Query(50, le=500),
    period_from: str | None = None,
    period_to: str | None = None,
):
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, f"Repository {owner}/{name} not found")

    filters = ["cs.repo_id = $1"]
    params: list = [repo["id"]]
    if period_from:
        params.append(period_from)
        filters.append(f"cs.period >= ${len(params)}")
    if period_to:
        params.append(period_to)
        filters.append(f"cs.period <= ${len(params)}")

    where = " AND ".join(filters)
    rows = await p.fetch(
        f"""
        SELECT c.github_login, c.email,
               cs.period, cs.commit_count, cs.insertions, cs.deletions,
               cs.files_changed
        FROM commit_stats cs
        JOIN contributors c ON c.id = cs.contributor_id
        WHERE {where}
        ORDER BY cs.commit_count DESC
        LIMIT ${len(params)+1}
        """,
        *params,
        limit,
    )
    return [dict(r) for r in rows]


# ── Project health score ───────────────────────────────────────────────────
@app.get("/repos/{owner}/{name}/health")
async def repo_health(owner: str, name: str):
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, "Repository not found")

    row = await p.fetchrow(
        """
        SELECT contributor_diversity, contributor_retention, collaboration,
               commit_consistency, bus_factor_safety, overall,
               bus_factor_count, top_contributor_pct, computed_at
        FROM health_scores
        WHERE repo_id = $1
        ORDER BY computed_at DESC
        LIMIT 1
        """,
        repo["id"],
    )

    if not row:
        raise HTTPException(404, "No health score computed yet for this repo")
    return dict(row)


# ── Bus factor ─────────────────────────────────────────────────────────────
@app.get("/repos/{owner}/{name}/busfactor")
async def bus_factor(owner: str, name: str, limit: int = Query(20, le=100)):
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, "Repository not found")

    rows = await p.fetch(
        """
        SELECT module_path, bus_factor, top_owner, top_owner_pct, computed_at
        FROM bus_factor
        WHERE repo_id = $1
        ORDER BY top_owner_pct DESC
        LIMIT $2
        """,
        repo["id"],
        limit,
    )
    return [dict(r) for r in rows]


# ── Collaboration graph ────────────────────────────────────────────────────
@app.get("/repos/{owner}/{name}/graph")
async def collab_graph(owner: str, name: str, min_weight: int = 1):
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, "Repository not found")

    rows = await p.fetch(
        """
        SELECT dev_a, dev_b, SUM(weight) AS weight, relationship
        FROM collaboration_edges
        WHERE repo_id = $1
        GROUP BY dev_a, dev_b, relationship
        HAVING SUM(weight) >= $2
        ORDER BY weight DESC
        LIMIT 2000
        """,
        repo["id"],
        min_weight,
    )

    nodes = set()
    edges = []
    for r in rows:
        nodes.add(r["dev_a"])
        nodes.add(r["dev_b"])
        edges.append(
            {
                "source": r["dev_a"],
                "target": r["dev_b"],
                "weight": r["weight"],
                "type": r["relationship"],
            }
        )

    return {
        "nodes": [{"id": n} for n in sorted(list(nodes))],
        "edges": edges,
    }


# ── Commit type distribution ───────────────────────────────────────────────
@app.get("/repos/{owner}/{name}/commit-types")
async def commit_types(owner: str, name: str, year: int | None = None):
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, "Repository not found")

    params: list = [repo["id"]]
    year_filter = ""
    if year:
        params.append(str(year))
        year_filter = f"AND EXTRACT(YEAR FROM period) = ${len(params)}"

    rows = await p.fetch(
        f"""
        SELECT commit_type, SUM(count) AS total
        FROM commit_types
        WHERE repo_id = $1 {year_filter}
        GROUP BY commit_type
        ORDER BY total DESC
        """,
        *params,
    )
    return [dict(r) for r in rows]


# ── Full Commit History & Author Search (Lazy Loading & Paged) ─────────────
_HDFS_PARQUET_CACHE: dict[str, tuple[float, pa.Table]] = {}


def get_hdfs_parquet_table(owner: str, name: str) -> pa.Table | None:
    """Reads commits.parquet from HDFS and caches in memory for fast paging & search."""
    cache_key = f"{owner}__{name}"
    now = time.time()
    if cache_key in _HDFS_PARQUET_CACHE:
        ts, tbl = _HDFS_PARQUET_CACHE[cache_key]
        if now - ts < 600:  # 10 minute cache
            return tbl

    hdfs_path = f"/raw/git_events/repository={owner}__{name}/commits.parquet"
    try:
        client = get_hdfs_client()
        with client.read(hdfs_path) as reader:
            bio = io.BytesIO(reader.read())
            table = pq.read_table(bio)
            # Ensure commit_type column exists
            if "commit_type" not in table.column_names and "subject" in table.column_names:
                subjects = table["subject"].to_pylist()
                c_types = [classify_subject(s) for s in subjects]
                table = table.append_column("commit_type", pa.array(c_types))
            _HDFS_PARQUET_CACHE[cache_key] = (now, table)
            return table
    except Exception as e:
        print(f"[HDFS Read] Commits Parquet not accessible for {owner}/{name}: {e}")
        return None


@app.get("/repos/{owner}/{name}/commits")
async def get_commits(
    owner: str,
    name: str,
    page: int = Query(1, ge=1, description="Page number, 1-indexed"),
    limit: int = Query(50, ge=1, le=200, description="Page size (items per page)"),
    author: str | None = Query(None, description="Filter by author name or email"),
    search: str | None = Query(None, description="Search term in commit message, hash, or author"),
    commit_type: str | None = Query(None, description="Filter by commit taxonomy type"),
    sort: str = Query("desc", description="Sort order by author_date ('desc' or 'asc')"),
):
    """
    Returns paginated commit history with support for lazy loading,
    author filtering, taxonomy classification filtering, and keyword search.
    """
    p = await pool()
    repo = await p.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if not repo:
        raise HTTPException(404, "Repository not found")

    repo_id = repo["id"]
    sort_dir = "ASC" if sort.lower() == "asc" else "DESC"

    # 1. Query indexed PostgreSQL commits table
    db_count = await p.fetchval("SELECT count(*) FROM commits WHERE repo_id=$1", repo_id)

    if db_count and db_count > 0:
        conditions = ["repo_id = $1"]
        params: list[Any] = [repo_id]

        if author and author.strip():
            params.append(f"%{author.strip()}%")
            conditions.append(f"(author_name ILIKE ${len(params)} OR author_email ILIKE ${len(params)})")

        if commit_type and commit_type.strip().upper() not in ("ALL", ""):
            params.append(commit_type.strip().upper())
            conditions.append(f"commit_type = ${len(params)}")

        if search and search.strip():
            s = f"%{search.strip()}%"
            params.append(s)
            conditions.append(f"(subject ILIKE ${len(params)} OR hash ILIKE ${len(params)} OR author_name ILIKE ${len(params)})")

        where_clause = " AND ".join(conditions)
        total_matching = await p.fetchval(f"SELECT count(*) FROM commits WHERE {where_clause}", *params)

        offset = (page - 1) * limit
        page_params = list(params)
        page_params.extend([limit, offset])

        rows = await p.fetch(
            f"""
            SELECT hash, author_name, author_email, author_date, subject,
                   commit_type, insertions, deletions, files_changed
            FROM commits
            WHERE {where_clause}
            ORDER BY author_date {sort_dir} NULLS LAST, id {sort_dir}
            LIMIT ${len(page_params)-1} OFFSET ${len(page_params)}
            """,
            *page_params,
        )

        commits_data = [
            {
                "hash": r["hash"],
                "author_name": r["author_name"],
                "author_email": r["author_email"],
                "author_date": r["author_date"].isoformat() if r["author_date"] else None,
                "subject": r["subject"],
                "commit_type": r["commit_type"] or "OTHER",
                "insertions": int(r["insertions"] or 0),
                "deletions": int(r["deletions"] or 0),
                "files_changed": int(r["files_changed"] or 0),
            }
            for r in rows
        ]

        total_pages = math.ceil(total_matching / limit) if total_matching > 0 else 1
        return {
            "total": total_matching,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "has_more": (page * limit) < total_matching,
            "commits": commits_data,
        }

    # 2. Fallback: Query directly from HDFS Parquet lakehouse
    table = get_hdfs_parquet_table(owner, name)
    if table is not None:
        mask = None

        if author and author.strip():
            author_term = author.strip().lower()
            m_author = pc.match_substring(pc.utf8_lower(table["author_name"]), author_term)
            if "author_email" in table.column_names:
                m_email = pc.match_substring(pc.utf8_lower(table["author_email"]), author_term)
                m_author = pc.or_(m_author, m_email)
            mask = m_author

        if commit_type and commit_type.strip().upper() not in ("ALL", ""):
            m_type = pc.equal(table["commit_type"], commit_type.strip().upper())
            mask = m_type if mask is None else pc.and_(mask, m_type)

        if search and search.strip():
            s_term = search.strip().lower()
            m_sub = pc.match_substring(pc.utf8_lower(table["subject"]), s_term)
            m_hash = pc.match_substring(pc.utf8_lower(table["hash"]), s_term)
            m_combined = pc.or_(m_sub, m_hash)
            mask = m_combined if mask is None else pc.and_(mask, m_combined)

        if mask is not None:
            filtered_table = table.filter(mask)
        else:
            filtered_table = table

        if sort.lower() == "asc" and "author_date" in filtered_table.column_names:
            indices = pc.sort_indices(filtered_table, sort_keys=[("author_date", "ascending")])
            filtered_table = filtered_table.take(indices)

        total_matching = len(filtered_table)
        offset = (page - 1) * limit
        slice_len = min(limit, max(0, total_matching - offset))

        if slice_len > 0 and offset < total_matching:
            page_slice = filtered_table.slice(offset, slice_len)
            pylist = page_slice.to_pylist()
        else:
            pylist = []

        commits_data = []
        for r in pylist:
            a_date = r.get("author_date")
            if a_date is not None and hasattr(a_date, "isoformat"):
                a_date_str = a_date.isoformat()
            elif a_date is not None:
                a_date_str = str(a_date)
            else:
                a_date_str = None

            commits_data.append({
                "hash": str(r.get("hash", "")),
                "author_name": str(r.get("author_name", "Unknown")),
                "author_email": str(r.get("author_email")) if r.get("author_email") else None,
                "author_date": a_date_str,
                "subject": str(r.get("subject", "")),
                "commit_type": str(r.get("commit_type", "OTHER")),
                "insertions": int(r.get("insertions", 0) or 0),
                "deletions": int(r.get("deletions", 0) or 0),
                "files_changed": int(r.get("files_changed", 0) or 0),
            })

        total_pages = math.ceil(total_matching / limit) if total_matching > 0 else 1
        return {
            "total": total_matching,
            "page": page,
            "limit": limit,
            "total_pages": total_pages,
            "has_more": (page * limit) < total_matching,
            "commits": commits_data,
        }

    return {
        "total": 0,
        "page": page,
        "limit": limit,
        "total_pages": 0,
        "has_more": False,
        "commits": [],
    }


@app.get("/repos/{owner}/{name}/commit-authors")
async def get_commit_authors(
    owner: str,
    name: str,
    q: str | None = Query(None, description="Search filter for author name or email"),
    limit: int = Query(50, ge=1, le=200, description="Max authors to return"),
):
    """
    Search authors/persons who committed to this repository.
    Enables user to search every person across the entire commit history.
    """
    p = await pool()
    repo = await p.fetchrow("SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name)
    if not repo:
        raise HTTPException(404, "Repository not found")

    repo_id = repo["id"]
    q_filter = f"%{q.strip()}%" if q and q.strip() else None

    # Check commits table first
    db_count = await p.fetchval("SELECT count(*) FROM commits WHERE repo_id=$1", repo_id)
    if db_count and db_count > 0:
        if q_filter:
            rows = await p.fetch(
                """
                SELECT author_name AS name, author_email AS email, count(*) AS commit_count
                FROM commits
                WHERE repo_id = $1 AND (author_name ILIKE $2 OR author_email ILIKE $2)
                GROUP BY author_name, author_email
                ORDER BY commit_count DESC
                LIMIT $3
                """,
                repo_id, q_filter, limit,
            )
        else:
            rows = await p.fetch(
                """
                SELECT author_name AS name, author_email AS email, count(*) AS commit_count
                FROM commits
                WHERE repo_id = $1
                GROUP BY author_name, author_email
                ORDER BY commit_count DESC
                LIMIT $2
                """,
                repo_id, limit,
            )
        return [dict(r) for r in rows]

    # Query from contributors + commit_stats
    if q_filter:
        rows = await p.fetch(
            """
            SELECT c.github_login AS name, c.email, SUM(cs.commit_count) AS commit_count
            FROM commit_stats cs
            JOIN contributors c ON c.id = cs.contributor_id
            WHERE cs.repo_id = $1 AND (c.github_login ILIKE $2 OR c.email ILIKE $2)
            GROUP BY c.github_login, c.email
            ORDER BY commit_count DESC
            LIMIT $3
            """,
            repo_id, q_filter, limit,
        )
    else:
        rows = await p.fetch(
            """
            SELECT c.github_login AS name, c.email, SUM(cs.commit_count) AS commit_count
            FROM commit_stats cs
            JOIN contributors c ON c.id = cs.contributor_id
            WHERE cs.repo_id = $1
            GROUP BY c.github_login, c.email
            ORDER BY commit_count DESC
            LIMIT $2
            """,
            repo_id, limit,
        )
    return [dict(r) for r in rows]


# ── HDFS Status ────────────────────────────────────────────────────────────
@app.get("/hdfs/status")
async def hdfs_status():
    """
    Query the HDFS NameNode for cluster health and list stored raw Parquet files.
    Requires the `hdfs` Python library to be reachable from this container.
    """
    import hdfs as hdfs_lib

    hdfs_url  = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
    hdfs_user = os.getenv("HDFS_USER", "root")
    base_path = os.getenv("HDFS_BASE_PATH", "/raw")

    try:
        client = hdfs_lib.InsecureClient(hdfs_url, user=hdfs_user)
        # List all parquet files under /raw/git_events
        try:
            raw_files = client.list(f"{base_path}/git_events", status=True)
        except Exception:
            raw_files = []

        repos_on_hdfs = []
        for entry_name, entry_status in raw_files:
            try:
                parquet_files = client.list(
                    f"{base_path}/git_events/{entry_name}", status=True
                )
                for fname, fstatus in parquet_files:
                    repos_on_hdfs.append(
                        {
                            "path": f"{base_path}/git_events/{entry_name}/{fname}",
                            "size_bytes": fstatus.get("length", 0),
                            "size_mb": round(fstatus.get("length", 0) / (1024 * 1024), 2),
                            "modified": fstatus.get("modificationTime", 0),
                            "replication": fstatus.get("replication", 1),
                        }
                    )
            except Exception:
                pass

        # Check warehouse Iceberg tables
        try:
            warehouse_contents = client.list("/warehouse/data", status=False)
        except Exception:
            warehouse_contents = []

        return {
            "namenode_url": hdfs_url,
            "status": "reachable",
            "raw_parquet_files": repos_on_hdfs,
            "raw_total_files": len(repos_on_hdfs),
            "warehouse_namespaces": list(warehouse_contents),
        }

    except Exception as exc:
        return {
            "namenode_url": hdfs_url,
            "status": "unreachable",
            "error": str(exc),
            "raw_parquet_files": [],
        }


# ── Spark Status ───────────────────────────────────────────────────────────
@app.get("/spark/status")
async def spark_status():
    """
    Query the Spark master REST API (port 8080/json/) for cluster status,
    running applications, and completed jobs.
    """
    import httpx

    spark_master_ui = "http://spark-master:8080"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client_http:
            resp = await client_http.get(f"{spark_master_ui}/json/")
            resp.raise_for_status()
            data = resp.json()

        return {
            "status": "reachable",
            "master_url": data.get("url", "spark://spark-master:7077"),
            "alive_workers": data.get("aliveworkers", 0),
            "cores_used": data.get("coresused", 0),
            "cores_total": data.get("cores", 0),
            "memory_used_mb": data.get("memoryused", 0),
            "memory_total_mb": data.get("memory", 0),
            "active_apps": [
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "status": a.get("status"),
                    "cores": a.get("cores"),
                    "memory_per_executor_mb": a.get("memoryperexecutor"),
                    "submit_date": a.get("submitdate"),
                }
                for a in data.get("activeapps", [])
            ],
            "completed_apps": [
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "status": a.get("status"),
                    "duration_ms": a.get("duration"),
                    "submit_date": a.get("submitdate"),
                }
                for a in data.get("completedapps", [])[-10:]  # last 10
            ],
        }

    except Exception as exc:
        return {
            "status": "unreachable",
            "error": str(exc),
            "active_apps": [],
            "completed_apps": [],
        }


# ── Live Commit SSE Stream ─────────────────────────────────────────────────
@app.get("/events/stream")
async def live_event_stream(request: Request):
    """
    Server-Sent Events endpoint — streams live commit events to the browser.

    Each event is a JSON object:
      { repo, hash, short_hash, author, message, commit_type, committed_at }

    The browser connects with:
      const src = new EventSource('http://localhost:8000/events/stream')
      src.onmessage = (e) => { const commit = JSON.parse(e.data); ... }
    """
    sub_q: asyncio.Queue = asyncio.Queue(maxsize=200)
    _sse_subscribers.add(sub_q)

    async def generate() -> AsyncGenerator[str, None]:
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(sub_q.get(), timeout=20.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # Send a keepalive comment so the connection stays open
                    yield ": keepalive\n\n"
        finally:
            _sse_subscribers.discard(sub_q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )


# ── Streaming Tracked Repositories Management ──────────────────────────────
class StreamingRepoRequest(BaseModel):
    repo: str = Field(..., description="GitHub repository shorthand (owner/name) or full URL")


def _clean_repo_name(raw: str) -> str:
    cleaned = raw.strip().rstrip("/")
    if cleaned.startswith("https://github.com/"):
        cleaned = cleaned[len("https://github.com/"):]
    elif cleaned.startswith("http://github.com/"):
        cleaned = cleaned[len("http://github.com/"):]
    elif cleaned.startswith("git@github.com:"):
        cleaned = cleaned[len("git@github.com:"):]
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]
    parts = cleaned.strip("/").split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid repository format '{raw}'. Expected 'owner/repo'.")
    return f"{parts[0]}/{parts[1]}"


@app.get("/streaming/repos")
async def get_streaming_repos():
    """Returns list of repositories currently tracked by the live streaming Kafka pipeline."""
    p = await pool()
    try:
        rows = await p.fetch("SELECT repo FROM streaming_repos ORDER BY created_at ASC")
        repos = [r["repo"] for r in rows]
        if repos:
            return repos
    except Exception:
        pass

    try:
        rows = await p.fetch("SELECT full_name FROM repositories ORDER BY synced_at DESC LIMIT 3")
        return [r["full_name"] for r in rows] if rows else ["torvalds/linux"]
    except Exception:
        return ["torvalds/linux"]


@app.post("/streaming/repos")
async def add_streaming_repo(req: StreamingRepoRequest):
    """Add a repository to the live streaming Kafka pipeline."""
    try:
        repo = _clean_repo_name(req.repo)
    except ValueError as e:
        raise HTTPException(400, str(e))

    p = await pool()
    await p.execute(
        """
        INSERT INTO streaming_repos (repo)
        VALUES ($1)
        ON CONFLICT (repo) DO NOTHING
        """,
        repo,
    )
    rows = await p.fetch("SELECT repo FROM streaming_repos ORDER BY created_at ASC")
    return {"status": "ok", "tracked_repos": [r["repo"] for r in rows]}


@app.delete("/streaming/repos/{owner}/{name}")
async def delete_streaming_repo(owner: str, name: str):
    """Remove a repository from the live streaming Kafka pipeline."""
    repo = f"{owner}/{name}"
    p = await pool()
    await p.execute("DELETE FROM streaming_repos WHERE repo = $1", repo)
    rows = await p.fetch("SELECT repo FROM streaming_repos ORDER BY created_at ASC")
    return {"status": "ok", "tracked_repos": [r["repo"] for r in rows]}
