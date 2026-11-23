#!/bin/bash
# Verification rapide de toute la chaine labsecu (Docker).
cd "$(dirname "$0")/.." || exit 1
set -a; [ -f .env ] && . ./.env; set +a

echo "== Conteneurs =="
docker compose ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}"

echo; echo "== Kafka : topic security-logs =="
docker exec labsecu-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic security-logs | head -n 1

echo; echo "== Cassandra =="
docker exec labsecu-cassandra nodetool status | grep -E '^(UN|DN)'
docker exec labsecu-cassandra cqlsh -k securite_logs -e "DESCRIBE TABLES;"

echo; echo "== Elasticsearch =="
curl -s -u "elastic:${ELASTIC_PASSWORD}" "http://localhost:9200/_cluster/health?filter_path=status"; echo
curl -s -u "elastic:${ELASTIC_PASSWORD}" "http://localhost:9200/_cat/indices/securite-*?v&s=index"

echo; echo "== Kibana =="
curl -s -o /dev/null -w "HTTP %{http_code} (200 ou 401 = pret)\n" http://localhost:5601/api/status

echo; echo "== Spark : dernieres alertes =="
docker logs labsecu-spark 2>&1 | grep ALERTE | tail -n 10
