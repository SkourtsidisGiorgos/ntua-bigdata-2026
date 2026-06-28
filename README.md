# Large-Scale Data Management — Semester Project (LA Crime)

Apache Spark 3.5.8 / Hadoop 3.4.1 on the CSLab Kubernetes cluster, in Java.
Each query is split into a testable `compute(...)` method and a thin `main`.

## Layout

```
src/main/java/gr/ntua/dsml/
  common/    Datasets, Paths, TimeBucket, Geo, JoinStrategy, Bench, Output
  q1/        Q1Df, Q1DfUdf, Q1Rdd          (Req 2)
  q2/        Q2Df, Q2Sql                   (Req 3)
  q3/        Q3Df, Q3Rdd                   (Req 4 + 6)
  q4/        Q4Df                          (Req 5 + 6)
  fmt/       Convert (csv -> parquet)      (Req 1)
src/test/java/...                          local JUnit tests
scripts/     env.sh, setup_clients.sh, vpn_up.sh, put_hdfs.sh, submit.sh, logs.sh, test.sh
notebooks/   eda.ipynb                     exploratory data analysis
report/      report.tex (Greek, XeLaTeX)
```

All cluster coordinates live in `scripts/env.sh` (sourced by every script).

## One-time setup

```bash
scripts/setup_clients.sh      # installs JDK17 + Spark + Hadoop clients into $HOME
sudo scripts/vpn_up.sh        # opens the VPN and registers cluster DNS (hdfs-namenode)
```

## Build & upload the jar

```bash
scripts/put_hdfs.sh           # mvn package -> uploads target/bigdata.jar to HDFS
```

## Run on the cluster

`submit.sh` takes the class short name (the `gr.ntua.dsml.` prefix is added) and
reads `EXECUTORS` / `CORES` / `MEM` from the environment. Output goes to the driver
pod; fetch it with `logs.sh`.

```bash
# Req 2 — Query 1 @ 2 exec, 1 core, 2GB
scripts/submit.sh q1.Q1Df ; scripts/submit.sh q1.Q1DfUdf ; scripts/submit.sh q1.Q1Rdd
scripts/logs.sh q1df

# Req 3 — Query 2 @ 4 exec
EXECUTORS=4 scripts/submit.sh q2.Q2Df
EXECUTORS=4 scripts/submit.sh q2.Q2Sql

# Req 4 — Query 3 @ 3 exec
EXECUTORS=3 scripts/submit.sh q3.Q3Df
EXECUTORS=3 scripts/submit.sh q3.Q3Rdd

# Req 1 — format conversion + compare
EXECUTORS=4 scripts/submit.sh fmt.Convert
EXECUTORS=4 scripts/submit.sh q2.Q2Df hdfs://hdfs-namenode:9000/user/dsml00314/parquet/crime

# Req 5 — Query 4 scalability (vertical A / horizontal B)
EXECUTORS=2 CORES=1 MEM=2g scripts/submit.sh q4.Q4Df
EXECUTORS=2 CORES=2 MEM=4g scripts/submit.sh q4.Q4Df
EXECUTORS=2 CORES=4 MEM=8g scripts/submit.sh q4.Q4Df
EXECUTORS=4 CORES=2 MEM=4g scripts/submit.sh q4.Q4Df
EXECUTORS=8 CORES=1 MEM=2g scripts/submit.sh q4.Q4Df

# Req 6 — force join strategies (passes the hint as arg; prints explain())
EXECUTORS=3 scripts/submit.sh q3.Q3Df broadcast
EXECUTORS=3 scripts/submit.sh q3.Q3Df merge
EXECUTORS=3 scripts/submit.sh q3.Q3Df shuffle_hash
EXECUTORS=3 scripts/submit.sh q3.Q3Df shuffle_replicate_nl
scripts/submit.sh q4.Q4Df broadcast
scripts/submit.sh q4.Q4Df shuffle_replicate_nl
```

## Local tests

```bash
scripts/test.sh               # JUnit + local Spark, under JDK17
```

Pure-logic tests (TimeBucket, Geo) and per-query logic on fixtures always run.
Loader tests against `./data` run only if the datasets have been downloaded locally.

## Report

```bash
cd report && latexmk -xelatex report.tex
```
