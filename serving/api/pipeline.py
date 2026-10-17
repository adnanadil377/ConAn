"""
Repository Ingestion & Analytics Pipeline Engine

Pipeline stages:
  1. Clone GitHub repository (blobless, no depth limit)
  2. Extract full commit history & file-level diff stats
  3. Write raw Parquet to HDFS  (hdfs://namenode:9000/raw/git_events/...)
  4. Compute analytics in Python  → sync serving tables in PostgreSQL
  5. Trigger Apache Spark jobs via Docker exec (detached, background):
       a. CommitETL      – raw HDFS Parquet → Iceberg lakehouse tables
       b. GraphAnalytics – Iceberg commits  → PageRank / bus-factor Iceberg tables
"""
from __future__ import annotations

import io
import math
import os
import re
import subprocess
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import hdfs as hdfs_lib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import pyarrow as pa
import pyarrow.parquet as pq

# ── Environment & Config ───────────────────────────────────────────────────
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
HDFS_USER         = os.getenv("HDFS_USER", "root")
OUTPUT_BASE       = os.getenv("HDFS_BASE_PATH", "/raw")

POSTGRES_HOST  = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT  = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB    = os.getenv("POSTGRES_DB", "github_analytics")
POSTGRES_USER  = os.getenv("POSTGRES_USER", "analytics")
POSTGRES_PASS  = os.getenv("POSTGRES_PASSWORD", "analytics")

# Spark integration via Docker exec
SPARK_MASTER_CONTAINER = os.getenv("SPARK_MASTER_CONTAINER", "spark-master")
SPARK_JAR_LOCAL        = "/opt/spark/work-dir/target/scala-2.12/github-analytics-spark_2.12-0.1.0.jar"
SPARK_ICEBERG_PACKAGE  = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"


def get_hdfs_client() -> hdfs_lib.InsecureClient:
    """Return a WebHDFS client pointed at the NameNode."""
    return hdfs_lib.InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
    )


# ── URL & Token Helpers ────────────────────────────────────────────────────
def parse_repo_url(url: str) -> tuple[str, str]:
    """Extract owner and repo name from GitHub URL or shorthand."""
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]

    m_ssh = re.match(r"git@[^:]+:([^/]+)/(.+)$", cleaned)
    if m_ssh:
        return m_ssh.group(1), m_ssh.group(2)

    m_http = re.match(r"https?://[^/]+/([^/]+)/(.+)$", cleaned)
    if m_http:
        return m_http.group(1), m_http.group(2)

    parts = cleaned.split("/")
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]

    raise ValueError(f"Could not parse repository owner and name from URL: '{url}'")


def build_authenticated_url(owner: str, name: str, token: str | None = None) -> str:
    """Build clone URL, injecting token securely if provided."""
    if token and token.strip():
        return f"https://x-access-token:{token.strip()}@github.com/{owner}/{name}.git"
    return f"https://github.com/{owner}/{name}.git"


def sanitize_message(msg: str, token: str | None = None) -> str:
    """Mask token from any string before logging or writing to DB."""
    if token and token.strip() and token.strip() in msg:
        return msg.replace(token.strip(), "***REDACTED***")
    return msg


# ── Git Operations ─────────────────────────────────────────────────────────
GIT_LOG_FORMAT = (
    "%H\x1f"   # commit hash
    "%ae\x1f"  # author email
    "%an\x1f"  # author name
    "%ce\x1f"  # committer email
    "%ai\x1f"  # author date ISO
    "%ci\x1f"  # committer date ISO
    "%P\x1f"   # parent hashes (space-separated)
    "%s"        # subject (first line of commit message)
)


def detect_default_branch(clone_url: str) -> str:
    """Inspect remote HEAD to discover the default branch."""
    try:
        cmd = ["git", "ls-remote", "--symref", clone_url, "HEAD"]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        for line in res.stdout.splitlines():
            m = re.search(r"ref:\s+refs/heads/(\S+)", line)
            if m:
                return m.group(1).strip()
    except Exception:
        pass
    return "main"


def clone_repository(
    clone_url: str,
    target_dir: str,
    branch: str | None = None,
    token: str | None = None,
) -> str:
    """
    Perform blobless clone on the single branch with NO depth limit.
    Returns the actual branch name that was cloned.
    """
    cmd = [
        "git",
        "clone",
        "--filter=blob:none",
        "--single-branch",
        "--no-checkout",
    ]
    if branch and branch.strip():
        cmd.extend(["-b", branch.strip()])
    cmd.extend([clone_url, target_dir])

    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        err = sanitize_message(res.stderr, token)
        raise RuntimeError(f"Git clone failed: {err}")

    b_res = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=target_dir,
        capture_output=True,
        text=True,
    )
    cloned_branch = (
        b_res.stdout.strip()
        if b_res.returncode == 0 and b_res.stdout.strip()
        else (branch or "main")
    )
    return cloned_branch


def extract_commits(repo_dir: str) -> tuple[list[dict], dict[str, int], list[tuple[str, str, str]]]:
    """
    Extracts all commits without depth limit.
    Returns:
      - commits list (dict per commit)
      - file_modification_counts (for code hotspots)
      - file_author_records (for module bus factor and collaboration graph)
    """
    cmd = ["git", "log", f"--pretty=format:{GIT_LOG_FORMAT}", "--numstat"]
    proc = subprocess.Popen(
        cmd,
        cwd=repo_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    commits: list[dict] = []
    file_mods: dict[str, int] = defaultdict(int)
    file_authors: list[tuple[str, str, str]] = []   # (author_name, filepath, period_month)
    current: dict | None = None

    stdout = proc.stdout
    if stdout is None:
        raise RuntimeError("Failed to read git log output stream")

    for line in stdout:
        parts = line.rstrip("\r\n").split("\x1f")
        if len(parts) == 8:
            if current:
                commits.append(current)
            current = {
                "hash":            parts[0],
                "author_email":    parts[1],
                "author_name":     parts[2] or parts[1],
                "committer_email": parts[3],
                "author_date":     parts[4],
                "committer_date":  parts[5],
                "parents":         parts[6].split() if parts[6] else [],
                "subject":         parts[7],
                "insertions":      0,
                "deletions":       0,
                "files_changed":   0,
            }
        elif line.strip() and current is not None:
            cols = line.rstrip("\r\n").split("\t")
            if len(cols) == 3:
                ins  = int(cols[0]) if cols[0].isdigit() else 0
                dels = int(cols[1]) if cols[1].isdigit() else 0
                filepath = cols[2].strip()
                current["insertions"]    += ins
                current["deletions"]     += dels
                current["files_changed"] += 1
                if filepath:
                    file_mods[filepath] += 1
                    try:
                        month_str = current["author_date"][:7] + "-01"
                        file_authors.append((current["author_name"], filepath, month_str))
                    except Exception:
                        pass

    if current:
        commits.append(current)

    proc.wait()
    if proc.returncode != 0 and not commits:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"git log failed: {stderr}")

    return commits, file_mods, file_authors


# ── Commit Classification ──────────────────────────────────────────────────
CLASSIFICATION_RULES = [
    (re.compile(r"(?i)(fix|bug|patch|hotfix|error|exception|crash|defect|resolve)"), "BUG_FIX"),
    (re.compile(r"(?i)(feat|feature|add|implement|new|introduce|support)"),           "FEATURE"),
    (re.compile(r"(?i)(refactor|clean|restructure|reorganize|rename|move)"),          "REFACTOR"),
    (re.compile(r"(?i)(doc|docs|readme|changelog|comment|javadoc|kdoc)"),             "DOCUMENTATION"),
    (re.compile(r"(?i)(perf|performance|optim|speed|fast|latency|memory)"),           "PERFORMANCE"),
    (re.compile(r"(?i)(test|spec|coverage|unit|integration|e2e)"),                    "TEST"),
    (re.compile(r"(?i)(security|auth|cve|vuln|xss|sql.inject|sanitize)"),             "SECURITY"),
    (re.compile(r"(?i)(ci|cd|deploy|release|version|bump|publish|workflow)"),         "DEVOPS"),
]


def classify_subject(subject: str | None) -> str:
    if not subject:
        return "OTHER"
    for pattern, category in CLASSIFICATION_RULES:
        if pattern.search(subject):
            return category
    return "OTHER"


def detect_dominant_language(file_mods: dict[str, int]) -> str:
    """Infers primary language from touched file extensions."""
    ext_map = {
        ".py": "Python", ".js": "JavaScript", ".ts": "TypeScript",
        ".tsx": "TypeScript", ".jsx": "JavaScript", ".java": "Java",
        ".scala": "Scala", ".go": "Go", ".rs": "Rust",
        ".cpp": "C++", ".c": "C", ".cs": "C#", ".rb": "Ruby",
        ".php": "PHP", ".swift": "Swift", ".kt": "Kotlin",
    }
    counts: dict[str, int] = defaultdict(int)
    for path, count in file_mods.items():
        ext = Path(path).suffix.lower()
        if ext in ext_map:
            counts[ext_map[ext]] += count
    return max(counts.items(), key=lambda x: x[1])[0] if counts else "Generic"


# ── HDFS Parquet Upload ────────────────────────────────────────────────────
def upload_commits_to_hdfs(commits: list[dict], owner: str, name: str) -> str:
    """
    Serialize commits list to Parquet (snappy-compressed) and write to HDFS.
    Returns the HDFS path where the file was written.
    """
    df = pd.DataFrame(commits)
    df["commit_type"]    = df["subject"].apply(classify_subject)
    df["author_date"]    = pd.to_datetime(df["author_date"], utc=True).dt.tz_localize(None).astype("datetime64[us]")
    df["committer_date"] = pd.to_datetime(df["committer_date"], utc=True).dt.tz_localize(None).astype("datetime64[us]")
    df["parent_count"]   = df["parents"].apply(len)
    df["is_merge"]       = df["parent_count"] > 1
    df.drop(columns=["parents"], inplace=True)

    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy", coerce_timestamps="us")
    parquet_bytes = sink.getvalue().to_pybytes()

    hdfs_path = f"{OUTPUT_BASE}/git_events/repository={owner}__{name}/commits.parquet"
    client = get_hdfs_client()
    parent = str(Path(hdfs_path).parent)
    client.makedirs(parent)
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(parquet_bytes)

    return hdfs_path


# ── Spark Job Trigger ─────────────────────────────────────────────────────
def _build_spark_submit_cmd(job_class: str, extra_args: list[str] | None = None) -> list[str]:
    """Build a spark-submit command list for the given Spark job class."""
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--packages", SPARK_ICEBERG_PACKAGE,
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.lakehouse.type=hadoop",
        "--conf", "spark.sql.catalog.lakehouse.warehouse=hdfs://namenode:9000/warehouse/data",
        "--conf", "spark.hadoop.fs.defaultFS=hdfs://namenode:9000",
        "--conf", "spark.hadoop.dfs.client.use.datanode.hostname=true",
        "--conf", "spark.sql.shuffle.partitions=8",
        "--class", job_class,
        SPARK_JAR_LOCAL,
    ]
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def trigger_spark_job_async(job_class: str, extra_args: list[str] | None = None) -> None:
    """
    Fire-and-forget Spark job submission.
    Runs spark-submit inside the spark-master container via Docker exec.
    Executes in a background daemon thread so it does not block the API.
    """
    def _run():
        try:
            import docker
            client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
            container = client.containers.get(SPARK_MASTER_CONTAINER)
            cmd = _build_spark_submit_cmd(job_class, extra_args)
            print(f"[Spark] Triggering {job_class} on container '{SPARK_MASTER_CONTAINER}'")
            # exec_run with detach=False so we capture logs; runs in background thread
            exit_code, output = container.exec_run(cmd, demux=False, stdout=True, stderr=True)
            log_output = output.decode("utf-8", errors="replace") if output else ""
            print(f"[Spark] {job_class} exited with code {exit_code}")
            if log_output:
                for line in log_output.splitlines()[-40:]:   # last 40 lines
                    print(f"  [Spark stdout] {line}")
        except Exception as exc:
            print(f"[Spark] Failed to trigger {job_class}: {exc}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()


# ── Analytics Calculation & PostgreSQL Sync ──────────────────────────────
def sync_analytics_to_postgres(
    owner: str,
    name: str,
    commits: list[dict],
    file_mods: dict[str, int],
    file_authors: list[tuple[str, str, str]],
    default_branch: str,
    head_commit_hash: str,
    is_private: bool,
) -> None:
    """
    Compute all serving-layer analytics from raw commit records and upsert
    into PostgreSQL tables consumed by the FastAPI dashboard endpoints.

    Tables populated:
      repositories, contributors, commit_stats, commit_types,
      health_scores, bus_factor, collaboration_edges
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # 1. Upsert repository ─────────────────────────────────────────────
        dominant_lang = detect_dominant_language(file_mods)
        cur.execute(
            """
            INSERT INTO repositories (owner, name, stars, forks, language, default_branch, head_commit_hash, is_private, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (owner, name) DO UPDATE
            SET language         = EXCLUDED.language,
                default_branch   = EXCLUDED.default_branch,
                head_commit_hash = EXCLUDED.head_commit_hash,
                is_private       = EXCLUDED.is_private,
                synced_at        = NOW()
            RETURNING id;
            """,
            (owner, name, 0, 0, dominant_lang, default_branch, head_commit_hash, is_private),
        )
        repo_id = cur.fetchone()[0]

        # 2. Upsert contributors ───────────────────────────────────────────
        df = pd.DataFrame(commits)
        df["author_date_dt"] = pd.to_datetime(df["author_date"], utc=True)
        contrib_df = (
            df.groupby(["author_email", "author_name"])
            .agg(
                first_seen=("author_date_dt", "min"),
                last_seen=("author_date_dt", "max"),
                total_commits=("hash", "count"),
            )
            .reset_index()
        )

        contrib_map: dict[str, int] = {}
        for _, row in contrib_df.iterrows():
            login = row["author_name"]
            email = row["author_email"]
            cur.execute(
                """
                INSERT INTO contributors (github_login, email, first_seen, last_seen, total_commits)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (github_login) DO UPDATE
                SET last_seen = GREATEST(contributors.last_seen, EXCLUDED.last_seen),
                    first_seen = LEAST(contributors.first_seen, EXCLUDED.first_seen),
                    total_commits = contributors.total_commits + EXCLUDED.total_commits
                RETURNING id;
                """,
                (
                    login,
                    email,
                    row["first_seen"].date(),
                    row["last_seen"].date(),
                    int(row["total_commits"]),
                ),
            )
            cid = cur.fetchone()[0]
            contrib_map[login] = cid

        # 3. Monthly Commit Stats ──────────────────────────────────────────
        df["period"] = df["author_date_dt"].dt.to_period("M").dt.to_timestamp().dt.date
        stats = (
            df.groupby(["author_name", "period"])
            .agg(
                commit_count=("hash", "count"),
                insertions=("insertions", "sum"),
                deletions=("deletions", "sum"),
                files_changed=("files_changed", "sum"),
            )
            .reset_index()
        )

        stat_records = [
            (
                repo_id,
                contrib_map[row["author_name"]],
                row["period"],
                int(row["commit_count"]),
                int(row["insertions"]),
                int(row["deletions"]),
                int(row["files_changed"]),
            )
            for _, row in stats.iterrows()
            if row["author_name"] in contrib_map
        ]
        if stat_records:
            execute_values(
                cur,
                """
                INSERT INTO commit_stats (repo_id, contributor_id, period, commit_count, insertions, deletions, files_changed)
                VALUES %s
                ON CONFLICT (repo_id, contributor_id, period) DO UPDATE
                SET commit_count  = EXCLUDED.commit_count,
                    insertions    = EXCLUDED.insertions,
                    deletions     = EXCLUDED.deletions,
                    files_changed = EXCLUDED.files_changed;
                """,
                stat_records,
            )

        # 4. Commit Types Breakdown ────────────────────────────────────────
        df["commit_type"] = df["subject"].apply(classify_subject)
        type_stats = (
            df.groupby(["period", "commit_type"])
            .size()
            .reset_index(name="count")
        )
        type_records = [
            (repo_id, row["period"], row["commit_type"], int(row["count"]))
            for _, row in type_stats.iterrows()
        ]
        if type_records:
            execute_values(
                cur,
                """
                INSERT INTO commit_types (repo_id, period, commit_type, count)
                VALUES %s
                ON CONFLICT (repo_id, period, commit_type) DO UPDATE
                SET count = EXCLUDED.count;
                """,
                type_records,
            )

        # NOTE: Full commit history is stored exclusively in HDFS Parquet
        # (hdfs://namenode:9000/raw/git_events/repository=.../commits.parquet).
        # Writing every commit row into PostgreSQL would duplicate that data
        # unnecessarily for large repos (50k–200k+ commits). The /commits API
        # endpoint reads directly from HDFS Parquet as its primary source.

        # 5. Project Health Score ─────────────────────────────────────────
        total_commits = len(commits)
        author_commit_counts = df["author_name"].value_counts().to_dict()
        n_authors = len(author_commit_counts)

        # Diversity: normalized Shannon entropy
        if n_authors > 1 and total_commits > 0:
            entropy = -sum(
                (cnt / total_commits) * math.log(cnt / total_commits)
                for cnt in author_commit_counts.values()
            )
            max_entropy = math.log(n_authors)
            contributor_diversity = float(min(1.0, entropy / max_entropy if max_entropy > 0 else 0.5))
        else:
            contributor_diversity = 0.2

        sorted_counts = sorted(author_commit_counts.values(), reverse=True)
        top_contributor_pct = float(sorted_counts[0] / total_commits) if total_commits > 0 else 1.0
        bus_factor_safety   = float(max(0.0, 1.0 - top_contributor_pct))

        accum = 0
        bus_factor_count = 0
        for cnt in sorted_counts:
            accum += cnt
            bus_factor_count += 1
            if accum >= total_commits * 0.5:
                break

        # Commit Consistency (monthly variance)
        monthly_counts = df.groupby("period")["hash"].count()
        if len(monthly_counts) > 1:
            mean_m = monthly_counts.mean()
            std_m  = monthly_counts.std()
            cv     = (std_m / mean_m) if mean_m > 0 else 1.0
            commit_consistency = float(1.0 / (1.0 + cv))
        else:
            commit_consistency = 0.5

        # Contributor Retention (ratio with >1 active month)
        author_months = df.groupby("author_name")["period"].nunique()
        retained = (author_months > 1).sum()
        contributor_retention = float(retained / n_authors if n_authors > 0 else 0.0)

        # Collaboration Score
        collaboration = float(min(1.0, 0.4 + (bus_factor_count / max(1, n_authors)) * 0.6))

        # Overall composite 0-100 score
        overall = float(
            (
                contributor_diversity * 0.25
                + commit_consistency   * 0.20
                + contributor_retention * 0.20
                + bus_factor_safety   * 0.20
                + collaboration       * 0.15
            )
            * 100
        )

        cur.execute(
            """
            INSERT INTO health_scores (
                repo_id, computed_at, contributor_diversity, contributor_retention,
                collaboration, commit_consistency, bus_factor_safety, overall,
                bus_factor_count, top_contributor_pct
            )
            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                repo_id,
                contributor_diversity,
                contributor_retention,
                collaboration,
                commit_consistency,
                bus_factor_safety,
                round(overall, 1),
                bus_factor_count,
                round(top_contributor_pct, 4),
            ),
        )

        # 6. Bus Factor per Module ─────────────────────────────────────────
        module_author_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for author, fpath, _ in file_authors:
            parts = fpath.split("/")
            module = parts[0] if len(parts) == 1 else f"{parts[0]}/{parts[1]}"
            module_author_counts[module][author] += 1

        bus_records = []
        today_date = datetime.now(timezone.utc).date()
        for mod, authors_dict in module_author_counts.items():
            mod_total = sum(authors_dict.values())
            if mod_total < 3:
                continue
            sorted_mod_devs = sorted(authors_dict.items(), key=lambda x: x[1], reverse=True)
            top_dev, top_dev_cnt = sorted_mod_devs[0]
            top_dev_pct = top_dev_cnt / mod_total

            mod_accum = 0
            mod_bf = 0
            for _, dev_c in sorted_mod_devs:
                mod_accum += dev_c
                mod_bf += 1
                if mod_accum >= mod_total * 0.5:
                    break

            bus_records.append((repo_id, mod, mod_bf, top_dev, round(top_dev_pct, 4), today_date))

        bus_records.sort(key=lambda x: x[4], reverse=True)
        top_bus_records = bus_records[:30]
        if top_bus_records:
            execute_values(
                cur,
                """
                INSERT INTO bus_factor (repo_id, module_path, bus_factor, top_owner, top_owner_pct, computed_at)
                VALUES %s
                ON CONFLICT (repo_id, module_path, computed_at) DO UPDATE
                SET bus_factor    = EXCLUDED.bus_factor,
                    top_owner     = EXCLUDED.top_owner,
                    top_owner_pct = EXCLUDED.top_owner_pct;
                """,
                top_bus_records,
            )

        # 7. Collaboration Graph Edges ─────────────────────────────────────
        mod_month_authors: dict[tuple[str, str], set[str]] = defaultdict(set)
        for author, fpath, period_m in file_authors:
            parts = fpath.split("/")
            mod = parts[0]
            mod_month_authors[(mod, period_m)].add(author)

        pair_weights: dict[tuple[str, str, str], int] = defaultdict(int)
        for (mod, period_m), authors_set in mod_month_authors.items():
            if len(authors_set) > 1:
                authors_list = sorted(list(authors_set))
                for i in range(len(authors_list)):
                    for j in range(i + 1, min(i + 6, len(authors_list))):
                        dev_a, dev_b = authors_list[i], authors_list[j]
                        pair_weights[(dev_a, dev_b, period_m)] += 1

        edge_records = [
            (repo_id, dev_a, dev_b, weight, "same_module", period_m)
            for (dev_a, dev_b, period_m), weight in pair_weights.items()
        ]
        edge_records.sort(key=lambda x: x[3], reverse=True)
        top_edges = edge_records[:2000]
        if top_edges:
            execute_values(
                cur,
                """
                INSERT INTO collaboration_edges (repo_id, dev_a, dev_b, weight, relationship, period)
                VALUES %s
                ON CONFLICT (repo_id, dev_a, dev_b, relationship, period) DO UPDATE
                SET weight = EXCLUDED.weight;
                """,
                top_edges,
            )

        conn.commit()

    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        cur.close()
        conn.close()


# ── Job Status Tracking ────────────────────────────────────────────────────
def update_job_status(
    job_id: str,
    status: str,
    progress_pct: int,
    step_msg: str,
    total_commits: int = 0,
    error_msg: str | None = None,
) -> None:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        completed_clause = ", completed_at = NOW()" if status in ("completed", "failed") else ""
        cur.execute(
            f"""
            UPDATE ingestion_jobs
            SET status        = %s,
                progress_pct  = %s,
                current_step  = %s,
                total_commits = GREATEST(total_commits, %s),
                error_message = %s
                {completed_clause}
            WHERE id = %s::uuid;
            """,
            (status, progress_pct, step_msg, total_commits, error_msg, job_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to update job status: {e}")


# ── Main Orchestration Worker ──────────────────────────────────────────────
def run_ingestion_pipeline(
    job_id: str,
    repo_url: str,
    access_token: str | None = None,
    branch: str | None = None,
) -> None:
    """
    End-to-end repository ingestion and analytics pipeline:
      1. Clone repository (blobless, unlimited depth)
      2. Extract full commit history and file-level diffs
      3. Upload raw Parquet to HDFS
      4. Compute analytics (health, bus factor, graph) → sync to PostgreSQL
      5. Asynchronously trigger Spark ETL and GraphAnalytics jobs on HDFS data
    """
    try:
        owner, name = parse_repo_url(repo_url)
        is_private  = bool(access_token and access_token.strip())

        # Update initial job metadata
        conn = get_db_connection()
        cur  = conn.cursor()
        cur.execute(
            """
            UPDATE ingestion_jobs
            SET repo_owner = %s, repo_name = %s, branch = %s, started_at = NOW()
            WHERE id = %s::uuid;
            """,
            (owner, name, branch or "main", job_id),
        )
        conn.commit()
        cur.close()
        conn.close()

        clone_url     = build_authenticated_url(owner, name, access_token)
        display_branch = branch or "default branch"

        # Step 1: Clone repository ────────────────────────────────────────
        update_job_status(
            job_id=job_id,
            status="cloning",
            progress_pct=10,
            step_msg=f"Cloning '{owner}/{name}' ({display_branch}, blobless, no depth limit)...",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            actual_branch = clone_repository(clone_url, tmpdir, branch, access_token)

            # Step 2: Extract Commits ──────────────────────────────────────
            update_job_status(
                job_id=job_id,
                status="extracting",
                progress_pct=30,
                step_msg=f"Extracting full commit history and file diffs (branch: '{actual_branch}')...",
            )

            commits, file_mods, file_authors = extract_commits(tmpdir)
            total_commits = len(commits)
            if not commits:
                raise RuntimeError("No commits found in the repository main branch")

            head_commit = commits[0]["hash"]

            # Step 3: Write Parquet to HDFS ───────────────────────────────
            update_job_status(
                job_id=job_id,
                status="hdfs_write",
                progress_pct=55,
                step_msg=f"Writing {total_commits:,} commits as Parquet to HDFS (hdfs://namenode:9000/raw)...",
                total_commits=total_commits,
            )

            hdfs_path = upload_commits_to_hdfs(commits, owner, name)
            print(f"[HDFS] Parquet written: hdfs://namenode:9000{hdfs_path}")

            # Step 4: Analytics Enrichment & Serving Sync ─────────────────
            update_job_status(
                job_id=job_id,
                status="analytics",
                progress_pct=75,
                step_msg="Computing Project Health, Module Bus Factor, Collaboration Graph...",
                total_commits=total_commits,
            )

            sync_analytics_to_postgres(
                owner=owner,
                name=name,
                commits=commits,
                file_mods=file_mods,
                file_authors=file_authors,
                default_branch=actual_branch,
                head_commit_hash=head_commit,
                is_private=is_private,
            )

            # Step 5: Trigger Spark batch jobs (async, non-blocking) ───────
            update_job_status(
                job_id=job_id,
                status="spark_etl",
                progress_pct=90,
                step_msg=(
                    f"Triggering Spark CommitETL & GraphAnalytics on HDFS data "
                    f"({total_commits:,} commits) — check Spark UI :8080..."
                ),
                total_commits=total_commits,
            )

            # CommitETL: raw HDFS Parquet → Iceberg lakehouse tables
            trigger_spark_job_async("analytics.CommitETL")

            # GraphAnalytics: Iceberg commits → PageRank / bus-factor Iceberg tables
            # Runs after a short delay to allow CommitETL to finish writing
            def _delayed_graph():
                import time
                time.sleep(60)   # give CommitETL time to complete
                trigger_spark_job_async("analytics.GraphAnalytics")

            t = threading.Thread(target=_delayed_graph, daemon=True)
            t.start()

            # Step 5: Mark Completed ───────────────────────────────────────
            update_job_status(
                job_id=job_id,
                status="completed",
                progress_pct=100,
                step_msg=(
                    f"Ingestion complete — {total_commits:,} commits processed, "
                    f"Parquet on HDFS, Spark ETL running in background."
                ),
                total_commits=total_commits,
            )

    except Exception as exc:
        err_msg = sanitize_message(str(exc), access_token)
        print(f"Ingestion job {job_id} failed: {err_msg}")
        update_job_status(
            job_id=job_id,
            status="failed",
            progress_pct=0,
            step_msg="Ingestion failed",
            error_msg=err_msg,
        )
