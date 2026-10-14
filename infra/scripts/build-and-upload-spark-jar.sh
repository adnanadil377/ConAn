#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# build-and-upload-spark-jar.sh
#
# Build the Spark assembly JAR with all required dependencies (Iceberg, PG JDBC)
# and upload it to HDFS so spark-submit can reference it directly from hdfs://.
#
# Usage:
#   ./infra/scripts/build-and-upload-spark-jar.sh
#
# Prerequisites:
#   - sbt (Scala Build Tool) must be installed
#   - Docker stack must be running (docker compose up -d)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SPARK_DIR="${PROJECT_ROOT}/spark"
JAR_NAME="github-analytics-spark_2.12-0.1.0.jar"
ASSEMBLY_DIR="${SPARK_DIR}/target/scala-2.12"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  GitHub Analytics — Spark JAR Build & Upload"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# 1. Build with sbt assembly (creates fat jar with Iceberg + PG JDBC bundled)
echo ""
echo "➤ Building Spark assembly JAR (sbt assembly)..."
cd "${SPARK_DIR}"
sbt assembly

# 2. Find the built jar
JAR_PATH="${ASSEMBLY_DIR}/${JAR_NAME}"
if [ ! -f "${JAR_PATH}" ]; then
  echo "ERROR: JAR not found at ${JAR_PATH}"
  echo "  Try running: cd spark && sbt assembly"
  exit 1
fi

echo "  ✓ JAR built: ${JAR_PATH} ($(du -sh "${JAR_PATH}" | cut -f1))"

# 3. Upload to HDFS via docker exec
echo ""
echo "➤ Uploading JAR to HDFS (hdfs://namenode:9000/jars/${JAR_NAME})..."

# Copy JAR into the namenode container
docker cp "${JAR_PATH}" namenode:/tmp/"${JAR_NAME}"

# Create /jars directory in HDFS (if not exists) and upload
docker exec namenode bash -c "
  hdfs dfs -mkdir -p hdfs://namenode:9000/jars;
  hdfs dfs -put -f /tmp/${JAR_NAME} hdfs://namenode:9000/jars/${JAR_NAME};
  hdfs dfs -chmod 644 hdfs://namenode:9000/jars/${JAR_NAME};
  echo 'HDFS upload complete.'
  hdfs dfs -ls hdfs://namenode:9000/jars/
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✓ JAR available at hdfs://namenode:9000/jars/${JAR_NAME}"
echo ""
echo "  The API will now trigger Spark jobs automatically"
echo "  after each repository ingestion."
echo ""
echo "  Spark Web UI:    http://localhost:8080"
echo "  HDFS NameNode:   http://localhost:9870"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
