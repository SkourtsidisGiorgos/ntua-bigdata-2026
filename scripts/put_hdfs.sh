#!/usr/bin/env bash
# Build the jar and upload it to HDFS so cluster-mode spark-submit can fetch it.
# Also ensures the eventLog/output dirs exist. Self-contained:
#   ./scripts/put_hdfs.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh   # JAVA_HOME, HDFS_HOME, PATH

JAR=target/bigdata.jar
HDFS_CODE="$HDFS_HOME/code"

echo "==> Building jar"
mvn -q -DskipTests package

echo "==> Ensuring HDFS dirs"
hdfs dfs -mkdir -p "$HDFS_CODE" "$HDFS_HOME/logs" "$HDFS_HOME/output"

echo "==> Uploading $JAR -> $HDFS_CODE/bigdata.jar"
hdfs dfs -put -f "$JAR" "$HDFS_CODE/bigdata.jar"
echo "Done."
