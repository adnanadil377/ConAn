# ConAn : Contribution Analytics Lakehouse for Github

[![Hadoop HDFS](https://img.shields.io/badge/Storage-Apache%20Hadoop%20HDFS%203.3.6-red?logo=apachehadoop&logoColor=white)](https://hadoop.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Compute-Apache%20Spark%203.5.3-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Lakehouse-Apache%20Iceberg%201.4.3-blue?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache%20Kafka%207.5.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Serving%20DB-PostgreSQL%2015-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI%20ASGI-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![React 19](https://img.shields.io/badge/Frontend-React%2019%20%2B%20TypeScript-61DAFB?logo=react&logoColor=black)](https://react.dev/)
[![D3.js](https://img.shields.io/badge/Visualizations-D3.js%20v7-F9A03C?logo=d3.js&logoColor=white)](https://d3js.org/)
[![Docker](https://img.shields.io/badge/Orchestration-Docker%20Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

A full-stack, enterprise-grade Big Data Lakehouse platform for analyzing GitHub repository contribution patterns, developer collaboration topologies, module-level bus-factor vulnerabilities, and real-time commit streams.

> 📖 **Full Architectural & Systems Specification**: See [docs/PROJECT_ARCHITECTURE_AND_ROADMAP.md](docs/PROJECT_ARCHITECTURE_AND_ROADMAP.md) for an exhaustive guide detailing internal engine mechanics, end-to-end data flows, mathematical formulations, and data contracts.  
> 📑 **Final Project Report (Full 15-Page)**: See [docs/PROJECT_REPORT.md](docs/PROJECT_REPORT.md) for the complete academic and engineering report covering architecture, tool-by-tool justification, empirical benchmark results, engineering challenges, and team contribution breakdown.  
> 📄 **Condensed Project Report (5-Page PDF)**: See [docs/PROJECT_REPORT_SHORT.pdf](docs/PROJECT_REPORT_SHORT.pdf) (or Markdown [docs/PROJECT_REPORT_SHORT.md](docs/PROJECT_REPORT_SHORT.md), LaTeX [docs/short_report.tex](docs/short_report.tex)) for the formatted 5-page executive summary.  
> 📊 **Presentation Slides (LaTeX Beamer)**: See [docs/presentation.tex](docs/presentation.tex) for the complete slide deck covering architecture, benchmarks, challenges, and team contributions.

---

## 🌟 Key Highlights

- **Infinite-Depth Blobless Ingestion**: Clones complete repository commit DAGs without arbitrary depth limits using Git partial clones (`--filter=blob:none --no-checkout`), cutting bandwidth consumption by **95%+**.
- **Distributed Raw Storage (HDFS)**: Immutable raw commit logs stored as Snappy-compressed Apache Parquet on **Apache Hadoop HDFS**, delivering **>11:1 storage compression**.
- **ACID Lakehouse Table Format (Apache Iceberg)**: Snapshot-isolated, partitioned tables on HDFS with atomic compare-and-swap (CAS) commits and schema evolution, eliminating metastore bottlenecks.
- **Distributed Graph & Batch Analytics (Apache Spark & GraphX)**: Distributed **PageRank** and **Degree Centrality** over developer co-authorship networks, regex-based commit taxonomy classification, and monthly contributor rollups.
- **Sub-25ms Serving Layer**: **PostgreSQL 15** and **FastAPI** (`asyncpg`) serve pre-aggregated analytical indices at sub-25 millisecond latencies, decoupling analytical batch jobs from interactive user queries.
- **Real-Time Event Streaming**: **Apache Kafka** broker and background consumers poll GitHub events with conditional ETag caching, fan-out live updates to browsers via **Server-Sent Events (SSE)**, and flush micro-batch Parquet shards to HDFS.
- **Interactive D3 Physics Graph**: **React 19** and **D3.js** velocity Verlet force simulation modeling social developer clusters with interactive zoom, pan, and dragging.
- **Cluster Infrastructure Telemetry**: Real-time dashboard monitoring HDFS NameNode storage capacity, DataNode health, Spark Master cluster cores, active workers, and running applications.

---

## 🏗️ System Architecture

```
                                  ┌───────────────────────────────┐
                                  │      GitHub Repositories      │
                                  └──────────────┬────────────────┘
                                                 │
                                                 │ Git Smart Protocol (blobless clone)
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ FASTAPI APPLICATION ENGINE & ORCHESTRATOR (:8000)                                              │
│                                                                                                 │
│  1. Parse commits via ASCII unit separator (\x1f) without delimiter collision                  │
│  2. Serialize RecordBatches to Snappy-compressed Apache Parquet via PyArrow                    │
│  3. Calculate Shannon entropy, CV consistency, bus factor & collaboration edges                │
└───────────────────────┬───────────────────────────────────┬─────────────────────────────────────┘
                        │                                   │
       PyArrow Stream   │                                   │ Pre-aggregated serving sync
                        ▼                                   ▼
┌──────────────────────────────────────────────┐  ┌──────────────────────────────────────────────┐
│ APACHE HADOOP HDFS (:9000 / :9870)           │  │ POSTGRESQL 15 SERVING DATABASE (:5432)       │
│                                              │  │                                              │
│ /raw/git_events/repository=*/commits.parquet │  │ • repositories       • contributors          │
│ /warehouse/data/lakehouse/github/*           │  │ • commit_stats       • commit_types          │
│ /jars/github-analytics-spark-assembly.jar    │  │ • health_scores      • bus_factor            │
└───────────────────────▲──────────────────────┘  │ • collab_edges       • ingestion_jobs        │
                        │                         └──────────────────────▲───────────────────────┘
                        │ Distributed reads/writes                       │
                        │                                                │ Sub-25ms REST queries
┌───────────────────────┴──────────────────────┐                         │
│ APACHE SPARK & GRAPHX CLUSTER                │  ┌──────────────────────┴───────────────────────┐
│                                              │  │ REACT 19 + TYPESCRIPT + D3.JS DASHBOARD      │
│ • CommitETL: Raw Parquet → Iceberg Tables    │  │                                              │
│ • GraphAnalytics: GraphX PageRank & Degree   │  │ • Interactive D3 Force-Directed Graph        │
│   Centrality on developer collaboration      │  │ • Bus Factor Risk Table (Alerts for BF = 1)   │
│   networks                                   │  │ • Commit Analytics & Monthly Taxonomy         │
│                                              │  │ • Live Infrastructure Monitor (HDFS & Spark) │
│ Master (:7077 / :8080) | Worker (:8081)      │  │ • Real-time Event Stream (Kafka SSE Ticker)  │
└──────────────────────────────────────────────┘  └──────────────────────────────────────────────┘
```

### ⚡ Real-Time Streaming Pipeline
```
GitHub /events API ──(ETag Polling)──► Kafka Producer ──► Kafka Broker (github.events.raw)
                                                                 │
                                    ┌────────────────────────────┴───────────────────────────┐
                                    ▼                                                        ▼
                        FastAPI SSE Fan-Out                                       HDFS Micro-Batch Flusher
                     (Live UI Activity Ticker)                               (/raw/git_events/.../live_*.parquet)
```

---

## 🧭 Cluster Services & Port Reference

The complete distributed cluster is orchestrated via Docker Compose:

| Service Name | Container | Port(s) | Endpoint / URL | Purpose |
|---|---|---|---|---|
| **Frontend UI** | `frontend` | `3000` | [http://localhost:3000](http://localhost:3000) | React 19 interactive analytics dashboard |
| **FastAPI Backend** | `api` | `8000` | [http://localhost:8000/docs](http://localhost:8000/docs) | REST API, OpenAPI docs & ingestion engine |
| **HDFS NameNode** | `namenode` | `9870`, `9000` | [http://localhost:9870](http://localhost:9870) | HDFS Web UI (`:9870`) & RPC master (`:9000`) |
| **HDFS DataNode** | `datanode` | — | Internal | HDFS distributed raw block storage |
| **HDFS Initializer** | `hdfs-init` | — | Internal | Safe mode handler & directory bootstrapper |
| **Spark Master** | `spark-master` | `8080`, `7077`, `6066` | [http://localhost:8080](http://localhost:8080) | Spark Cluster Web UI (`:8080`) & RPC (`:7077`) |
| **Spark Worker** | `spark-worker-1` | `8081` | [http://localhost:8081](http://localhost:8081) | Worker executor status & resource telemetry |
| **PostgreSQL 15** | `postgres` | `5432` | `localhost:5432` | Serving database (`github_analytics`) |
| **Apache Kafka** | `kafka` | `9092`, `29092` | `localhost:9092` | Real-time event broker |
| **ZooKeeper** | `zookeeper` | `2181` | `localhost:2181` | Kafka cluster coordination & leader election |
| **Kafka UI** | `kafka-ui` | `8090` | [http://localhost:8090](http://localhost:8090) | Visual topic & consumer lag inspection |
| **Kafka Producer** | `kafka-producer` | — | Internal | GitHub Events API conditional poller |

---

## 🚀 Quickstart Guide

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (allocate 4GB+ RAM, 2+ CPUs)
- Optional: `sbt` (Scala Build Tool) if compiling the Spark assembly JAR locally (otherwise the automated build script handles it inside Docker).
- Optional: `node` 18+ & `npm` if running the frontend outside Docker.

### 1. Clone the Repository & Configure Environment
```bash
git clone https://github.com/adnanadil377/bigdata-pro.git
cd bigdata-pro

# Copy example environment configuration
cp .env.example .env
```
*(Optional: Set your `GITHUB_TOKEN` in `.env` to avoid public rate-limiting during large-scale testing).*

### 2. Start the Distributed Stack
Launch the HDFS, Spark, Kafka, PostgreSQL, and FastAPI services:
```bash
docker compose up -d
```
Verify all core containers are running:
```bash
docker compose ps
```

### 3. Build & Upload the Spark Assembly JAR to HDFS
Compile the Scala assembly fat JAR (bundling Apache Iceberg runtime and PostgreSQL JDBC) and upload it to `hdfs://namenode:9000/jars/`:
```bash
chmod +x ./infra/scripts/build-and-upload-spark-jar.sh
./infra/scripts/build-and-upload-spark-jar.sh
```

### 4. Launch the Frontend
You can launch the frontend via Docker or locally:

**Option A — Via Docker Profile (Recommended):**
```bash
docker compose --profile app up -d frontend
```

**Option B — Locally with Vite:**
```bash
cd frontend
npm install
npm run dev
```

The web dashboard is now accessible at **[http://localhost:3000](http://localhost:3000)**.

---

## 💻 Ingestion & Usage Walkthrough

### 1. Ingest a Repository via Dashboard
1. Open [http://localhost:3000](http://localhost:3000) in your browser.
2. Click **"Ingest Repository"** in the top navigation bar.
3. Enter a repository shorthand or URL (e.g., `expressjs/express`, `pallets/click`, or `facebook/react`).
4. (Optional) Provide a branch name and a GitHub Personal Access Token for private repositories.
5. Click **"Start Ingestion"**. The real-time progress modal tracks the 6-stage lifecycle:
   - `queued` $\rightarrow$ `cloning` $\rightarrow$ `extracting` $\rightarrow$ `hdfs_write` $\rightarrow$ `analytics` $\rightarrow$ `spark_etl` $\rightarrow$ `completed`.

### 2. Ingest via cURL / REST API
Trigger ingestion asynchronously via the REST endpoint:
```bash
curl -X POST http://localhost:8000/repos/ingest \
  -H "Content-Type: application/json" \
  -d '{"repo_url": "expressjs/express"}'
```
Response:
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "owner": "expressjs",
  "name": "express",
  "status": "queued",
  "message": "Ingestion started"
}
```

Poll job status and real-time step progress:
```bash
curl http://localhost:8000/repos/jobs/<job_id>
```

### 3. Explore Analytical Views
- **Overview Dashboard**: High-level repository KPIs, composite health gauge, and primary maintainers.
- **Collaboration Graph**: Interactive D3.js physics network. Nodes represent developers (radius proportional to degree centrality) and links represent co-authorship weight.
- **Bus Factor Risk Table**: Module-level bus factor rankings highlighting single points of failure ($\text{BusFactor} = 1$).
- **Commit Analytics & Taxonomy**: Paginated, searchable commit history reading directly from HDFS Parquet, paired with monthly stacked taxonomy charts.
- **Infrastructure Monitor**: Live telemetry for HDFS NameNode storage capacity, DataNodes, and Spark Master cluster resources.
- **Live Event Stream**: Real-time ticker streaming live GitHub pushes via Kafka and SSE.

---

## 📡 REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/repos/ingest` | Asynchronously initiates full-depth Git clone & HDFS pipeline |
| `GET` | `/repos/jobs/{id}` | Polls ingestion job status, progress percentage, and current step |
| `GET` | `/repos/jobs` | Lists the most recent ingestion jobs |
| `GET` | `/repos` | Lists all tracked repositories and synchronization timestamps |
| `GET` | `/repos/{owner}/{name}/overview` | Returns aggregate metrics, overall health score, and top maintainer |
| `GET` | `/repos/{owner}/{name}/health` | Detailed dimensional breakdown of health scores |
| `GET` | `/repos/{owner}/{name}/busfactor` | Module-level bus factor rankings and risk alerts |
| `GET` | `/repos/{owner}/{name}/graph` | Formatted node/link JSON for D3 force-directed visualization |
| `GET` | `/repos/{owner}/{name}/commits` | Paginated commit search reading directly from HDFS Parquet |
| `GET` | `/repos/{owner}/{name}/commit-authors` | Searchable list of all distinct commit authors in the repository |
| `GET` | `/repos/{owner}/{name}/commit-types` | Monthly distribution of commit taxonomy categories |
| `GET` | `/repos/{owner}/{name}/hotspots` | Top code modules ranked by churn frequency |
| `POST` | `/repos/{owner}/{name}/sync` | Triggers an incremental re-sync for a repository |
| `DELETE` | `/repos/{owner}/{name}` | Cascading deletion of a repository and all analytical indices |
| `GET` | `/hdfs/status` | NameNode cluster status, disk capacity, and stored Parquet files |
| `GET` | `/spark/status` | Spark master telemetry, worker cores, memory, and application runs |
| `GET` | `/streaming/repos` | Lists repositories currently monitored by the Kafka streaming daemon |
| `POST` | `/streaming/repos` | Dynamically adds a repository to the Kafka streaming poller |
| `DELETE` | `/streaming/repos/{owner}/{name}` | Removes a repository from the Kafka streaming poller |
| `GET` | `/realtime/events` | Server-Sent Events (SSE) live push stream for connected browsers |
| `GET` | `/health` | API service health and liveness probe |

---

## ⚙️ Apache Spark Jobs & Lakehouse Data Layout

### 1. Batch ETL (`analytics.CommitETL`)
- **Input Source**: `hdfs://namenode:9000/raw/git_events/repository=*/*.parquet`
- **Transformations**:
  - Extracts repository identifiers from HDFS partition paths (`repository=<owner>__<name>`).
  - Deduplicates commits across historical re-syncs and branches by `(repository, hash)`.
  - Classifies commit subjects via regex into semantic categories (`FEATURE`, `BUG_FIX`, `REFACTOR`, `DEVOPS`, etc.).
  - Aggregates monthly contributor rollups (`author_month`, `commit_count`, `insertions`, `deletions`).
- **Target Iceberg Tables**:
  - `lakehouse.github.commits`: Master deduplicated commit catalog.
  - `lakehouse.github.contributor_stats`: Monthly aggregated contributor metrics.
  - `lakehouse.github.commit_types`: Monthly taxonomy distribution.
  - `lakehouse.github.file_hotspots`: File modification frequencies.

### 2. Graph Analytics (`analytics.GraphAnalytics`)
- **Input Source**: `lakehouse.github.commits` (Iceberg)
- **Transformations**:
  - Maps contributor emails to 64-bit integer vertex IDs (`VertexRDD`).
  - Constructs collaboration edges by joining developers who touched the same repository in the same calendar month.
  - Executes distributed **GraphX PageRank** ($d=0.85$, $\epsilon=0.0001$) to identify core maintainers and structural influencers.
  - Computes **Degree Centrality** (number of distinct developer connections).
  - Calculates module-level bus factors by ranking developer commit shares.
- **Target Iceberg Tables**:
  - `lakehouse.github.graph_metrics`: Developer PageRank and degree centrality.
  - `lakehouse.github.bus_factor`: Module bus factor metrics.
  - `lakehouse.github.collab_edges`: Weighted co-authorship network edges.

### 3. Automated Containerized Execution
Upon completing stage 4 of ingestion, FastAPI automatically triggers `spark-submit` inside the `spark-master` container via the mounted Docker socket (`/var/run/docker.sock`):
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=hdfs://namenode:9000/warehouse/data \
  --class analytics.CommitETL \
  /opt/spark/work-dir/target/scala-2.12/github-analytics-spark_2.12-0.1.0.jar
```
*(Package downloads are cached in the persistent `spark-ivy-cache` Docker volume, ensuring instant subsequent executions).*

---

## 📐 Mathematical Metrics & Formulations

The platform computes rigorous mathematical indicators to assess open-source repository sustainability:

### 1. Contributor Diversity (Normalized Shannon Entropy)
Quantifies whether commit activity is distributed across the community or monopolized by a few contributors:
$$H_{\text{norm}} = -\frac{1}{\ln(N)} \sum_{i=1}^{N} p_i \ln(p_i) \quad \in [0.0, 1.0]$$
- $N$: Total number of distinct contributors.
- $p_i = \frac{c_i}{C_{\text{total}}}$: Proportion of commits authored by contributor $i$.
- $H_{\text{norm}} \to 1.0$: Healthy, decentralized distribution; $H_{\text{norm}} \to 0.0$: Monopolistic contributor ownership.

### 2. Commit Consistency (Coefficient of Variation)
Measures the predictability and regularity of engineering velocity over monthly intervals:
$$CV = \frac{\sigma_{\text{monthly}}}{\mu_{\text{monthly}}}, \qquad \text{Score}_{\text{consistency}} = \frac{1.0}{1.0 + CV}$$
Where $\mu_{\text{monthly}}$ and $\sigma_{\text{monthly}}$ represent the mean and standard deviation of monthly commit volumes.

### 3. Module-Level Bus Factor
Identifies the minimum number of developers whose departure would jeopardize module maintenance:
$$\text{BusFactor}(M) = \min \left\{ k : \sum_{i=1}^{k} c_i \ge 0.50 \cdot C_{\text{module}} \right\}$$
- Contributors sorted in descending order of commit volume ($c_1 \ge c_2 \ge \dots \ge c_m$).
- Modules with $\text{BusFactor} = 1$ and $C_{\text{module}} \ge 5$ trigger automated high-priority warning badges in the UI.

### 4. Distributed PageRank on Developer Networks
$$PR(u) = \frac{1 - d}{|V|} + d \sum_{v \in B_u} \frac{PR(v)}{L(v)}$$
Where $d = 0.85$, $B_u$ is the set of developers collaborating with $u$, and $L(v)$ is the outbound collaboration count of $v$. High PageRank identifies foundational architectural maintainers.

---

## 📁 Repository Directory Structure

```
bigdata-pro/
├── docker-compose.yml              # Multi-container orchestration (10 services)
├── README.md                       # Master project documentation
├── .env.example                    # Environment variable template
├── docs/
│   ├── PROJECT_ARCHITECTURE_AND_ROADMAP.md # In-depth systems and architecture spec
│   └── PROJECT_REPORT.md           # Academic & engineering final report
├── infra/
│   ├── postgres/
│   │   └── init.sql                # PostgreSQL serving layer schema and indices
│   └── scripts/
│       └── build-and-upload-spark-jar.sh # sbt build & HDFS JAR staging script
├── ingestion/
│   └── streaming/
│       ├── Dockerfile              # Kafka producer container image
│       ├── kafka_producer.py       # GitHub Events conditional ETag poller
│       └── kafka_consumer.py       # Kafka consumer with SSE bridge & HDFS flusher
├── serving/
│   └── api/
│       ├── Dockerfile              # FastAPI backend container image
│       ├── main.py                 # REST API endpoints, SSE streams & status proxies
│       └── pipeline.py             # Git clone, PyArrow Parquet, HDFS write & Spark exec
├── spark/
│   ├── build.sbt                   # Scala 2.12 build definition with Iceberg & PG JDBC
│   ├── etl/src/main/scala/analytics/
│   │   └── CommitETL.scala         # Spark SQL batch ETL & Iceberg writer
│   └── graph/src/main/scala/analytics/
│       └── GraphAnalytics.scala    # Spark GraphX PageRank, Centrality & Bus Factor
└── frontend/
    ├── package.json                # React 19, Vite 6, TypeScript, D3 dependencies
    ├── vite.config.ts              # Vite config with API proxy
    └── src/
        ├── App.tsx                 # Main application component & layout
        ├── components/
        │   ├── CollaborationGraph.tsx    # D3 force-directed interactive physics graph
        │   ├── InfrastructureMonitor.tsx # HDFS & Spark live cluster telemetry
        │   ├── BusFactorRiskTable.tsx    # Single-maintainer risk alerts
        │   ├── CommitAnalyticsView.tsx   # Searchable commit history & taxonomy charts
        │   ├── HealthScorePanel.tsx      # Multi-dimensional health radial gauge
        │   ├── IngestionModal.tsx        # Ingestion dialog with real-time state machine
        │   └── LiveEventStream.tsx       # Real-time SSE ticker from Kafka
        └── services/
            └── api.ts              # Typed API client for FastAPI backend
```

---

## 🛠️ Verification & Operational Commands

```bash
# 1. Verify container cluster health
docker compose ps

# 2. Check HDFS status via API
curl -s http://localhost:8000/hdfs/status | jq .

# 3. Check Spark cluster status via API
curl -s http://localhost:8000/spark/status | jq .

# 4. Inspect HDFS filesystem contents directly
docker exec namenode hdfs dfs -ls /raw/git_events/
docker exec namenode hdfs dfs -ls /warehouse/data/lakehouse/github/
docker exec namenode hdfs dfs -ls /jars/

# 5. List ingested repositories in PostgreSQL via API
curl -s http://localhost:8000/repos | jq .

# 6. Fetch repository overview and health score
curl -s http://localhost:8000/repos/expressjs/express/overview | jq .

# 7. Query collaboration graph edges
curl -s "http://localhost:8000/repos/expressjs/express/graph?min_weight=2" | jq .

# 8. View live Kafka messages via browser
open http://localhost:8090
```

---

## 📄 License
This project is open-source and available under the [MIT License](LICENSE).
