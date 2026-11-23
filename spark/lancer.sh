#!/bin/bash
# Lance le job Spark avec les connecteurs Kafka, Cassandra et Elasticsearch.
# Les .jar sont telecharges au premier demarrage puis gardes dans le volume spark_ivy.
set -e
PAQUETS="org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}"
PAQUETS="${PAQUETS},com.datastax.spark:spark-cassandra-connector_2.12:${CASSANDRA_CONNECTOR_VERSION}"
PAQUETS="${PAQUETS},org.elasticsearch:elasticsearch-spark-30_2.12:${ES_SPARK_VERSION}"

exec /opt/spark/bin/spark-submit \
  --master "local[4]" \
  --driver-memory 1g \
  --packages "${PAQUETS}" \
  --conf spark.jars.ivy=/opt/spark/ivy \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.session.timeZone=UTC \
  --py-files /opt/labsecu/traitement_securite.py,/opt/labsecu/regles_detection.py \
  /opt/labsecu/job_securite.py
