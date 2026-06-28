#!/usr/bin/env bash
# Parametrized spark-submit for the CSLab k8s cluster. One launcher drives every
# executor-config matrix in the assignment (reqs 2-5). eventLog is on so each run
# leaves a trace in the Spark History Server / HDFS (required deliverable).
#
# Usage (give only the class shortname; gr.ntua.dsml. is prepended):
#   ./scripts/submit.sh Smoke
#   EXECUTORS=4 MEM=2g ./scripts/submit.sh q2.Q2Df <appArgs...>
#   BUILD=1 ./scripts/submit.sh q1.Q1Df          # rebuild+upload jar first
#
# Env knobs (defaults shown): EXECUTORS=2 CORES=1 MEM=2g DRIVER_MEM=1g
#   IMAGE=apache/spark:3.5.8-java17  WAIT=true  BUILD=false  NAME=<derived>
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh

BASE_PKG=gr.ntua.dsml
CLASS=${1:?usage: submit.sh <ClassShortName> [appArgs...]}; shift
[[ $CLASS == ${BASE_PKG}.* ]] && FQCN=$CLASS || FQCN=${BASE_PKG}.${CLASS}

EXECUTORS=${EXECUTORS:-2}
CORES=${CORES:-1}
MEM=${MEM:-2g}
DRIVER_MEM=${DRIVER_MEM:-1g}
# Off-heap headroom (MiB) so multi-core executors aren't OOMKilled by k8s. This is
# separate from spark.executor.memory (the heap the assignment specifies); scales
# with cores since concurrent tasks each use shuffle/netty buffers.
OVERHEAD=${OVERHEAD:-$(( CORES * 512 ))}
IMAGE=${IMAGE:-apache/spark:3.5.8-java17}
WAIT=${WAIT:-true}
NAME=${NAME:-$(echo "${CLASS##*.}" | tr '[:upper:]' '[:lower:]')-e${EXECUTORS}-c${CORES}}

[[ ${BUILD:-} == 1 || ${BUILD:-} == true ]] && ./scripts/put_hdfs.sh

echo "==> submit  $FQCN  exec=${EXECUTORS}x${CORES}core/${MEM}  name=$NAME"

spark-submit \
  --master "$K8S_MASTER" \
  --deploy-mode cluster \
  --name "$NAME" \
  --class "$FQCN" \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.namespace="$K8S_NAMESPACE" \
  --conf spark.kubernetes.driverEnv.HADOOP_USER_NAME="$HADOOP_USER_NAME" \
  --conf spark.executorEnv.HADOOP_USER_NAME="$HADOOP_USER_NAME" \
  --conf spark.kubernetes.container.image="$IMAGE" \
  --conf spark.kubernetes.submission.waitAppCompletion="$WAIT" \
  --conf spark.executor.instances="$EXECUTORS" \
  --conf spark.executor.cores="$CORES" \
  --conf spark.executor.memory="$MEM" \
  --conf spark.executor.memoryOverhead="$OVERHEAD" \
  --conf spark.driver.memory="$DRIVER_MEM" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="$HDFS_HOME/logs" \
  --conf spark.history.fs.logDirectory="$HDFS_HOME/logs" \
  "$HDFS_HOME/code/bigdata.jar" "$@"
