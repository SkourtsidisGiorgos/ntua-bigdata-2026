# Source this (don't execute) to put Spark/Hadoop clients on PATH for the CSLab cluster.
#   source scripts/env.sh
export HADOOP_USER_NAME=dsml00314
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME="$HOME/spark-3.5.8-bin-hadoop3"
export HADOOP_HOME="$HOME/hadoop-3.4.1"
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH"

# Cluster coordinates (used by submit scripts)
export K8S_MASTER="k8s://https://termi7.cslab.ece.ntua.gr:6443"
export K8S_NAMESPACE="dsml00314-priv"
export HDFS_NAMENODE="hdfs://hdfs-namenode:9000"
export HDFS_HOME="$HDFS_NAMENODE/user/dsml00314"
