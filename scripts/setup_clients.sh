#!/usr/bin/env bash
# One-time setup of local Spark + Hadoop clients for the CSLab k8s cluster.
# Installs to $HOME (per lab guide). Uses openjdk-17 for Spark (system Java 25 left untouched).
# Idempotent: safe to re-run.
set -euo pipefail

SPARK_VER=3.5.8
HADOOP_VER=3.4.1
SPARK_DIR="$HOME/spark-${SPARK_VER}-bin-hadoop3"
HADOOP_DIR="$HOME/hadoop-${HADOOP_VER}"
JDK_HOME=/usr/lib/jvm/java-17-openjdk-amd64
NAMENODE="hdfs://hdfs-namenode:9000"

echo "==> 1/4 JDK17 (apt)"
if [ ! -d "$JDK_HOME" ]; then
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jdk
fi

echo "==> 2/4 Spark ${SPARK_VER}"
if [ ! -d "$SPARK_DIR" ]; then
  cd "$HOME"
  wget -q --show-progress "https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop3.tgz"
  tar -xzf "spark-${SPARK_VER}-bin-hadoop3.tgz"
  rm -f "spark-${SPARK_VER}-bin-hadoop3.tgz"
fi

echo "==> 3/4 Hadoop ${HADOOP_VER}"
if [ ! -d "$HADOOP_DIR" ]; then
  cd "$HOME"
  wget -q --show-progress "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VER}/hadoop-${HADOOP_VER}.tar.gz"
  tar -xzf "hadoop-${HADOOP_VER}.tar.gz"
  rm -f "hadoop-${HADOOP_VER}.tar.gz"
fi

echo "==> 4/4 HDFS client config (core-site.xml -> ${NAMENODE})"
cat > "${HADOOP_DIR}/etc/hadoop/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>${NAMENODE}</value>
  </property>
</configuration>
EOF

echo
echo "Done. From the repo root, to use the clients in a shell, run:"
echo "    source scripts/env.sh"
