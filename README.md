# GitHub Contribution Analytics — Big Data Lakehouse

A full-stack big data analytics platform for GitHub repository contribution patterns, built on **HDFS + Apache Spark + Apache Iceberg + PostgreSQL**.

> 📖 **Full Architectural & Systems Specification**: See [docs/PROJECT_ARCHITECTURE_AND_ROADMAP.md](file:///Users/adnan/Documents/bigdata-pro/docs/PROJECT_ARCHITECTURE_AND_ROADMAP.md) for an in-depth guide detailing how every technology works under the hood, their exact project roles, end-to-end data flows, mathematical formulations, and data schemas.

## Architecture

```
GitHub Repo
    │
    ▼ (blobless clone, unlimited depth)
┌─────────────┐
│  FastAPI    │  Git clone → extract commits
│  API        │  ─────────────────────────────────────► HDFS
│ :8000       │  /raw/git_events/repository=owner__name/commits.parquet
└─────────────┘
    │
    │ Python analytics (health, bus factor, graph edges)
    ▼
┌─────────────┐
│ PostgreSQL  │  Serving layer — fast queries for dashboard
│ :5432       │
└─────────────┘
    │
    │ (background) Docker exec → spark-submit
    ▼
┌─────────────┐   CommitETL         ┌───────────────────────┐
│ Spark Master│──────────────────►  │  HDFS Iceberg Tables  │
│ :7077/:8080 │   GraphAnalytics    │  /warehouse/data/      │
└─────────────┘                     │  lakehouse.github.*    │
                                    └───────────────────────┘
```

### Services
| Service | Port | Purpose |
|---------|------|---------|
| FastAPI | :8000 | REST API, ingestion orchestration |
| Spark Master | :8080 | Spark Web UI |
| Spark Worker | :8081 | Worker node |
| Spark RPC | :7077 | Spark cluster endpoint |
| HDFS NameNode | :9870 | HDFS Web UI |
| HDFS RPC | :9000 | HDFS client endpoint |
| PostgreSQL | :5432 | Serving database |
| Kafka UI | :8090 | Kafka monitoring (future) |

### Pipeline Stages
1. **cloning** — Git blobless clone (no depth limit)
2. **extracting** — Parse full commit history + file-level diffs
3. **hdfs_write** — Write Parquet to `hdfs://namenode:9000/raw/git_events/`
4. **analytics** — Compute health, bus factor, collaboration graph → PostgreSQL
5. **spark_etl** — Trigger `CommitETL` + `GraphAnalytics` on HDFS data → Iceberg
6. **completed** — Dashboard ready, Spark continues in background

## Quickstart

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- sbt (for building Spark JAR)

### 1. Start the stack
```bash
docker compose up -d
```

### 2. Build and upload the Spark JAR to HDFS
```bash
./infra/scripts/build-and-upload-spark-jar.sh
```

This builds the assembly JAR (Iceberg + PG JDBC bundled) and uploads to HDFS.

### 3. Start the frontend (optional)
```bash
cd frontend && npm install && npm run dev
# or with Docker:
docker compose --profile app up frontend
```

### 4. Ingest a repository
```bash
curl -X POST http://localhost:8000/repos/ingest \
  -H 'Content-Type: application/json' \
  -d '{"repo_url": "facebook/react"}'
```

Watch progress:
```bash
curl http://localhost:8000/repos/jobs/<job_id>
```

### 5. Monitor
- **Dashboard**: http://localhost:3000
- **Spark UI**: http://localhost:8080
- **HDFS UI**: http://localhost:9870
- **API docs**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8090

## API Endpoints

```
POST /repos/ingest               Ingest a GitHub repository
GET  /repos/jobs/{id}            Poll ingestion job status
GET  /repos/jobs                 List recent jobs
GET  /repos                      List tracked repositories
GET  /repos/{owner}/{name}/overview    Dashboard data
GET  /repos/{owner}/{name}/graph       Collaboration graph
GET  /repos/{owner}/{name}/health      Health score
GET  /repos/{owner}/{name}/busfactor   Bus factor per module
GET  /repos/{owner}/{name}/commit-types Commit taxonomy
POST /repos/{owner}/{name}/sync        Re-sync
DELETE /repos/{owner}/{name}           Delete
GET  /hdfs/status                HDFS NameNode status + file list
GET  /spark/status               Spark cluster status + completed jobs
```

## Spark Jobs

### CommitETL (`analytics.CommitETL`)
- Reads: `hdfs://namenode:9000/raw/git_events/**/*.parquet`
- Writes Iceberg tables:
  - `lakehouse.github.commits` — deduplicated commit records
  - `lakehouse.github.contributor_stats` — monthly rollups
  - `lakehouse.github.commit_types` — commit classification by type

### GraphAnalytics (`analytics.GraphAnalytics`)
- Reads: `lakehouse.github.commits` (Iceberg)
- Runs: GraphX PageRank + degree centrality
- Writes Iceberg tables:
  - `lakehouse.github.graph_metrics` — PageRank + degree per developer
  - `lakehouse.github.bus_factor` — bus factor per repository
  - `lakehouse.github.collab_edges` — weighted collaboration network

## Spark Job Trigger

After HDFS upload, the API triggers Spark jobs via Docker exec:
```python
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  --class analytics.CommitETL \
  /opt/spark/work-dir/target/scala-2.12/github-analytics-spark_2.12-0.1.0.jar
```

The first run downloads the Iceberg package (~50MB) to the Spark container's Ivy cache (persisted in the `spark-ivy-cache` Docker volume). Subsequent runs are instant.

## What Was Removed
- ❌ ML retention prediction (`ContributorMLPipeline.scala`, `retention_predictions` table)
- ❌ Kafka live stream tab (deferred — Kafka services remain in docker-compose)
- ❌ `realtime_events` table

## What Was Added / Fixed
- ✅ HDFS properly integrated (raw Parquet writes + status API)
- ✅ Spark jobs triggered automatically via Docker exec after ingestion
- ✅ Iceberg lakehouse warehouse at `hdfs://namenode:9000/warehouse/data`
- ✅ Infrastructure Monitor tab (HDFS + Spark status in dashboard)
- ✅ `/hdfs/status` API endpoint
- ✅ `/spark/status` API endpoint (Spark master REST API)
- ✅ Pipeline stages updated: `hdfs_write` → `analytics` → `spark_etl`
