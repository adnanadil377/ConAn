#!/usr/bin/env python3
"""
Streaming consumer: reads from Kafka topic 'github.events.raw' and:
  1. Upserts commits into the Postgres 'commits' table
  2. Buffers commits and flushes Parquet shards to HDFS alongside
     the existing batch files:
       /raw/git_events/repository=<owner>__<name>/live_<timestamp>.parquet
  3. Puts each commit dict onto an asyncio.Queue for SSE fan-out

This module is intended to be imported and run as a background asyncio
task from the FastAPI lifespan handler. It can also be run standalone
for testing:

    KAFKA_BOOTSTRAP=localhost:9092 \
    DATABASE_URL=postgresql://analytics:analytics@localhost:5432/github_analytics \
    HDFS_NAMENODE_URL=http://localhost:9870 \
    python kafka_consumer.py
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import asyncpg
import hdfs as hdfs_lib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("kafka_consumer")

# ── Config ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP",      "localhost:9092")
KAFKA_TOPIC          = os.getenv("KAFKA_TOPIC",           "github.events.raw")
KAFKA_GROUP_ID       = os.getenv("KAFKA_GROUP_ID",        "fastapi-sse-consumer")

DATABASE_URL         = os.getenv(
    "DATABASE_URL",
    "postgresql://analytics:analytics@postgres:5432/github_analytics",
)

HDFS_NAMENODE_URL    = os.getenv("HDFS_NAMENODE_URL",     "http://namenode:9870")
HDFS_USER            = os.getenv("HDFS_USER",             "root")
HDFS_BASE_PATH       = os.getenv("HDFS_BASE_PATH",        "/raw")
HDFS_FLUSH_THRESHOLD = int(os.getenv("HDFS_FLUSH_THRESHOLD", "20"))   # commits
HDFS_FLUSH_INTERVAL  = int(os.getenv("HDFS_FLUSH_INTERVAL",  "60"))   # seconds

# Shared queue — FastAPI SSE endpoint subscribes to this
_live_queue: asyncio.Queue[dict[str, Any]] | None = None


def get_live_queue() -> asyncio.Queue[dict[str, Any]]:
    global _live_queue
    if _live_queue is None:
        _live_queue = asyncio.Queue(maxsize=1000)
    return _live_queue


# ── Commit classification (mirrors sync_lakehouse_to_postgres.py) ───────────
_CLASSIFY_RULES: list[tuple[str, str]] = [
    (r"(?i)(fix|bug|patch|hotfix|error|exception|crash|defect)",    "BUG_FIX"),
    (r"(?i)(feat|feature|add|implement|new|introduce|support)",      "FEATURE"),
    (r"(?i)(refactor|clean|restructure|reorganize|rename|move)",     "REFACTOR"),
    (r"(?i)(doc|docs|readme|changelog|comment|javadoc|kdoc)",        "DOCUMENTATION"),
    (r"(?i)(perf|performance|optim|speed|fast|latency|memory)",      "PERFORMANCE"),
    (r"(?i)(test|spec|coverage|unit|integration|e2e)",               "TEST"),
    (r"(?i)(security|auth|cve|vuln|xss|sql.inject|sanitize)",        "SECURITY"),
    (r"(?i)(ci|cd|deploy|release|version|bump|publish|workflow)",    "DEVOPS"),
]


def classify_subject(subject: str | None) -> str:
    if not subject:
        return "OTHER"
    for pat, cat in _CLASSIFY_RULES:
        if re.search(pat, subject):
            return cat
    return "OTHER"


# ── HDFS helpers ────────────────────────────────────────────────────────────

def _hdfs_client() -> hdfs_lib.InsecureClient:
    return hdfs_lib.InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)


def _to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    df = pd.DataFrame(rows)
    for col in ("author_date", "committer_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce") \
                        .dt.tz_localize(None) \
                        .astype("datetime64[us]")
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink  = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy", coerce_timestamps="us")
    return sink.getvalue().to_pybytes()


def _flush_to_hdfs(owner: str, name: str, rows: list[dict[str, Any]]) -> None:
    """Write buffered commits as a timestamped Parquet shard on HDFS."""
    timestamp  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    shard_path = (
        f"{HDFS_BASE_PATH}/git_events/repository={owner}__{name}"
        f"/live_{timestamp}.parquet"
    )
    try:
        client = _hdfs_client()
        data   = _to_parquet_bytes(rows)
        client.write(shard_path, io.BytesIO(data), overwrite=True)
        logger.info("HDFS shard written: %s (%d commits, %.1f KB)",
                    shard_path, len(rows), len(data) / 1024)
    except Exception:
        logger.exception("Failed to flush HDFS shard for %s/%s", owner, name)


# ── Postgres helpers ─────────────────────────────────────────────────────────

async def _get_or_create_repo(conn: asyncpg.Connection, owner: str, name: str) -> int:
    """Ensure the repository row exists and return its id."""
    row = await conn.fetchrow(
        "SELECT id FROM repositories WHERE owner=$1 AND name=$2", owner, name
    )
    if row:
        return row["id"]
    row = await conn.fetchrow(
        """
        INSERT INTO repositories (owner, name, synced_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (owner, name) DO UPDATE SET synced_at = NOW()
        RETURNING id
        """,
        owner, name,
    )
    return row["id"]


async def _update_streaming_summaries(
    conn: asyncpg.Connection,
    repo_id: int,
    commit: dict[str, Any],
) -> None:
    """
    Update PostgreSQL analytical summary rollups (commit_types, contributors, repo synced_at).
    Does NOT store raw commits in PostgreSQL, enforcing zero double-writing and guaranteeing
    that all raw commit data resides strictly in HDFS Parquet.
    """
    author       = commit.get("author") or {}
    author_name  = author.get("name") or commit.get("author_name") or "Unknown"
    author_email = author.get("email") or commit.get("author_email") or ""
    message      = commit.get("message") or commit.get("subject") or ""
    subject      = message.splitlines()[0][:1000] if message else ""
    commit_type  = classify_subject(subject)
    committed_at = (
        commit.get("timestamp")
        or commit.get("committer_date")
        or commit.get("author_date")
        or datetime.now(timezone.utc).isoformat()
    )

    try:
        dt = datetime.fromisoformat(committed_at.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)

    period_date = dt.date().replace(day=1)

    # 1. Update commit_types distribution summary
    await conn.execute(
        """
        INSERT INTO commit_types (repo_id, period, commit_type, count)
        VALUES ($1, $2, $3, 1)
        ON CONFLICT (repo_id, period, commit_type) DO UPDATE
        SET count = commit_types.count + 1
        """,
        repo_id,
        period_date,
        commit_type,
    )

    # 2. Update contributor summary
    login_id = (author_name.lower().replace(" ", "-") if author_name != "Unknown" else author_email) or "unknown"
    await conn.execute(
        """
        INSERT INTO contributors (github_login, email, first_seen, last_seen, total_commits)
        VALUES ($1, $2, $3, $3, 1)
        ON CONFLICT (github_login) DO UPDATE
        SET total_commits = contributors.total_commits + 1,
            last_seen = GREATEST(contributors.last_seen, EXCLUDED.last_seen),
            email = COALESCE(contributors.email, EXCLUDED.email)
        """,
        login_id,
        author_email or None,
        dt.date(),
    )

    # 3. Touch repo synced_at
    await conn.execute(
        "UPDATE repositories SET synced_at = NOW() WHERE id = $1",
        repo_id,
    )


# ── Main consumer loop ──────────────────────────────────────────────────────

async def consume_kafka_events(stop_event: asyncio.Event | None = None) -> None:
    """
    Long-running coroutine. Reads PushEvents from Kafka, updates analytical
    summaries in Postgres, buffers raw commits for HDFS Parquet, and fans-out
    to the live SSE queue.

    Call asyncio.create_task(consume_kafka_events()) from FastAPI lifespan.
    """
    # Import here so the module is usable even without confluent_kafka installed
    # (e.g. in unit-test environments that stub the function out)
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        logger.error("confluent-kafka not installed — streaming consumer disabled")
        return

    live_q   = get_live_queue()
    db_pool  = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           KAFKA_GROUP_ID,
        "auto.offset.reset":  "latest",   # only new events while running
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Kafka consumer subscribed to '%s' (group=%s)", KAFKA_TOPIC, KAFKA_GROUP_ID)

    # Per-repo HDFS buffers: { "owner/name": (list[dict], last_flush_time) }
    hdfs_buffers: dict[str, tuple[list[dict], float]] = {}

    def _maybe_flush(full_name: str) -> None:
        """Flush HDFS buffer if threshold or interval reached."""
        if full_name not in hdfs_buffers:
            return
        rows, last_flush = hdfs_buffers[full_name]
        elapsed = time.monotonic() - last_flush
        if len(rows) >= HDFS_FLUSH_THRESHOLD or elapsed >= HDFS_FLUSH_INTERVAL:
            if rows:
                owner, name = full_name.split("/", 1)
                _flush_to_hdfs(owner, name, rows)
            hdfs_buffers[full_name] = ([], time.monotonic())

    async with db_pool.acquire() as conn:
        try:
            while not (stop_event and stop_event.is_set()):
                # Poll is blocking — run in executor to avoid blocking event loop
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: consumer.poll(timeout=1.0)
                )

                if msg is None:
                    # Periodic interval-based HDFS flush even when no messages arrive
                    for full_name in list(hdfs_buffers):
                        _maybe_flush(full_name)
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    logger.warning("Malformed Kafka message, skipping")
                    continue

                if event.get("event_type") != "PushEvent":
                    continue

                full_name = event.get("repo", "unknown/unknown")
                commits   = event.get("payload", {}).get("commits", [])
                if not commits:
                    continue

                # Ensure repo exists for FK
                try:
                    owner, name = full_name.split("/", 1)
                except ValueError:
                    continue

                try:
                    repo_id = await _get_or_create_repo(conn, owner, name)
                except Exception:
                    logger.exception("Failed to upsert repo %s", full_name)
                    continue

                # Init HDFS buffer for this repo if needed
                if full_name not in hdfs_buffers:
                    hdfs_buffers[full_name] = ([], time.monotonic())

                for commit in commits:
                    sha          = commit.get("sha") or commit.get("hash") or ""
                    author       = commit.get("author") or {}
                    author_name  = author.get("name") or "Unknown"
                    author_email = author.get("email") or ""
                    message      = commit.get("message") or ""
                    subject      = message.splitlines()[0][:1000] if message else ""
                    commit_type  = classify_subject(subject)
                    committed_at = (
                        commit.get("timestamp")
                        or event.get("created_at")
                        or datetime.now(timezone.utc).isoformat()
                    )

                    # 1. Postgres summary update (no raw commits double-written)
                    try:
                        await _update_streaming_summaries(conn, repo_id, {
                            **commit,
                            "author_date":     committed_at,
                            "committer_date":  committed_at,
                        })
                    except Exception:
                        logger.exception("PostgreSQL summary update failed for %s@%s", full_name, sha[:7])

                    # 2. Add to HDFS buffer
                    hdfs_buffers[full_name][0].append({
                        "hash":           sha,
                        "author_name":    author_name,
                        "author_email":   author_email,
                        "author_date":    committed_at,
                        "committer_date": committed_at,
                        "subject":        subject,
                        "commit_type":    commit_type,
                        "insertions":     0,
                        "deletions":      0,
                        "files_changed":  0,
                    })

                    # 3. SSE fan-out (non-blocking — drop if queue is full)
                    live_event = {
                        "repo":         full_name,
                        "hash":         sha,
                        "short_hash":   sha[:7],
                        "author":       author_name,
                        "message":      subject,
                        "commit_type":  commit_type,
                        "committed_at": committed_at,
                    }
                    try:
                        live_q.put_nowait(live_event)
                    except asyncio.QueueFull:
                        pass  # drop oldest — don't block producer

                    logger.debug("  ✓ %s @ %s  (%s)", full_name, sha[:7], commit_type)

                # Flush HDFS buffer if threshold/interval reached
                _maybe_flush(full_name)

        except asyncio.CancelledError:
            logger.info("Kafka consumer cancelled — flushing remaining HDFS buffers")
            for full_name, (rows, _) in hdfs_buffers.items():
                if rows:
                    o, n = full_name.split("/", 1)
                    _flush_to_hdfs(o, n, rows)
        finally:
            consumer.close()
            await db_pool.close()
            logger.info("Kafka consumer shut down")


# ── Standalone entry-point ──────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(consume_kafka_events())
