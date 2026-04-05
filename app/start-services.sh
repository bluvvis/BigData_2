#!/bin/bash
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
mapred --daemon start historyserver

jps -lm

echo "HDFS status (best-effort, max 90s)..."
if command -v timeout >/dev/null 2>&1; then
    timeout 90 hdfs dfsadmin -report || echo "WARN: dfsadmin -report timed out or failed; continuing."
else
    hdfs dfsadmin -report || true
fi

hdfs dfsadmin -safemode leave

hdfs dfs -mkdir -p /apps/spark
SPARK_JAR_ZIP_LOCAL="/tmp/spark-yarn-jars.zip"
SPARK_JAR_ZIP_HDFS="/apps/spark/spark-yarn-jars.zip"
if hdfs dfs -test -f "${SPARK_JAR_ZIP_HDFS}" 2>/dev/null; then
  echo "Spark jar archive already in HDFS (${SPARK_JAR_ZIP_HDFS}), skipping upload."
else
  echo "Packaging /usr/local/spark/jars into ${SPARK_JAR_ZIP_LOCAL} ..."
  rm -f "${SPARK_JAR_ZIP_LOCAL}"
  if command -v zip >/dev/null 2>&1; then
    (cd /usr/local/spark/jars && zip -q -r "${SPARK_JAR_ZIP_LOCAL}" . -i '*.jar')
    echo "Uploading single archive to HDFS ${SPARK_JAR_ZIP_HDFS} ..."
    hdfs dfs -put -f "${SPARK_JAR_ZIP_LOCAL}" "${SPARK_JAR_ZIP_HDFS}"
  else
    echo "WARN: zip not found; falling back to per-jar upload (slow on Mac Docker)."
    hdfs dfs -mkdir -p /apps/spark/jars
    hdfs dfs -chmod 744 /apps/spark/jars
    hdfs dfs -put /usr/local/spark/jars/* /apps/spark/jars/
    hdfs dfs -chmod +rx /apps/spark/jars/
  fi
fi

mkdir -p /app/spark-conf
FS_DEFAULT="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null | tr -d '\r' | head -1)"
if [ -z "${FS_DEFAULT}" ]; then
  FS_DEFAULT="hdfs://cluster-master:9000"
fi
ARCH_URI="${FS_DEFAULT}${SPARK_JAR_ZIP_HDFS}"
cat > /app/spark-conf/yarn-archive.conf <<EOF
spark.yarn.archive ${ARCH_URI}
EOF
echo "Wrote /app/spark-conf/yarn-archive.conf with spark.yarn.archive=${ARCH_URI}"


scala -version
jps -lm
hdfs dfs -mkdir -p /user/root

