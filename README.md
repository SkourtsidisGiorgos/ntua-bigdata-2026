# Big Data semester project - LA Crime dataset

Spark 3.5.8 / Hadoop 3.4.1 on CSLab Kubernetes. Written in Java.

Each query has a `compute(...)` method (testable) and a thin `main`.

```
src/main/java/gr/ntua/dsml/
  common/    Datasets, Paths, TimeBucket, Geo, JoinStrategy, Bench, Output
  q1/        Q1Df, Q1DfUdf, Q1Rdd
  q2/        Q2Df, Q2Sql
  q3/        Q3Df, Q3Rdd
  q4/        Q4Df
  fmt/       Convert (csv -> parquet)
src/test/java/...
scripts/     env.sh, setup_clients.sh, vpn_up.sh, put_hdfs.sh, submit.sh, logs.sh, test.sh
notebooks/   eda.ipynb
report/      report.tex (XeLaTeX)
```

Cluster settings are in `scripts/env.sh` (sourced by everything).

## Setup

Run once:

```bash
scripts/setup_clients.sh   # JDK17, Spark, Hadoop clients
sudo scripts/vpn_up.sh      # VPN + DNS for hdfs-namenode
```

## Build and run

```bash
scripts/put_hdfs.sh        # mvn package, uploads jar to HDFS
```

`submit.sh` takes the short class name (adds `gr.ntua.dsml.`). Set `EXECUTORS` / `CORES` / `MEM` to configure resources. Output goes to driver pod; grab it with `logs.sh`.

```bash
# Query 1 (2 exec, 1 core, 2GB)
scripts/submit.sh q1.Q1Df ; scripts/submit.sh q1.Q1DfUdf ; scripts/submit.sh q1.Q1Rdd
scripts/logs.sh q1df

# Query 2 (4 exec)
EXECUTORS=4 scripts/submit.sh q2.Q2Df
EXECUTORS=4 scripts/submit.sh q2.Q2Sql

# Query 3 (3 exec)
EXECUTORS=3 scripts/submit.sh q3.Q3Df
EXECUTORS=3 scripts/submit.sh q3.Q3Rdd

# CSV -> Parquet conversion
EXECUTORS=4 scripts/submit.sh fmt.Convert
EXECUTORS=4 scripts/submit.sh q2.Q2Df hdfs://hdfs-namenode:9000/user/dsml00314/parquet/crime

# Query 4 - scalability tests
EXECUTORS=2 CORES=1 MEM=2g scripts/submit.sh q4.Q4Df
EXECUTORS=2 CORES=2 MEM=4g scripts/submit.sh q4.Q4Df
EXECUTORS=2 CORES=4 MEM=8g scripts/submit.sh q4.Q4Df
EXECUTORS=4 CORES=2 MEM=4g scripts/submit.sh q4.Q4Df
EXECUTORS=8 CORES=1 MEM=2g scripts/submit.sh q4.Q4Df

# Force join strategies
EXECUTORS=3 scripts/submit.sh q3.Q3Df broadcast
EXECUTORS=3 scripts/submit.sh q3.Q3Df merge
EXECUTORS=3 scripts/submit.sh q3.Q3Df shuffle_hash
EXECUTORS=3 scripts/submit.sh q3.Q3Df shuffle_replicate_nl
scripts/submit.sh q4.Q4Df broadcast
scripts/submit.sh q4.Q4Df shuffle_replicate_nl
```

## Local testing

```bash
scripts/test.sh    # JUnit with local Spark
```

Tests for TimeBucket/Geo always run. Data loader tests need `./data` populated.

## Report

```bash
cd report && latexmk -xelatex report.tex
```
