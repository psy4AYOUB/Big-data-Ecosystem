#!/bin/bash
# Configuration initiale d'Elasticsearch (executee une fois par le conteneur es-setup) :
#   - mot de passe du compte kibana_system
#   - role spark_writer + compte spark_es (ecriture dans securite-*)
#   - modeles d'index securite-evenements-* et securite-alertes-*
set -euo pipefail
ES="http://elasticsearch:9200"
AUTH="elastic:${ELASTIC_PASSWORD}"

appel() {   # appel METHODE CHEMIN [fichier_json | -d donnees]
  local methode=$1 chemin=$2; shift 2
  local code
  code=$(curl -s -o /tmp/reponse -w '%{http_code}' -u "$AUTH" -X "$methode" "$ES$chemin" \
         -H 'Content-Type: application/json' "$@")
  if [[ "$code" != 2* ]]; then echo "ERREUR $code sur $methode $chemin"; cat /tmp/reponse; echo; exit 1; fi
  echo "OK  $methode $chemin"
}

echo "Attente d'Elasticsearch..."
until curl -s -u "$AUTH" "$ES/_cluster/health" | grep -qE '"status":"(green|yellow)"'; do sleep 5; done

appel POST "/_security/user/kibana_system/_password" -d "{\"password\":\"${KIBANA_SYSTEM_PASSWORD}\"}"

# "monitor" : le connecteur elasticsearch-spark lit la version du cluster (cluster:monitor/main)
# "maintenance" : il rafraichit l index apres chaque ecriture (indices:admin/refresh)
appel PUT "/_security/role/spark_writer" -d '{
  "cluster": ["monitor"],
  "indices": [ { "names": ["securite-*"],
                 "privileges": ["create_index", "auto_configure", "write", "create_doc", "index", "view_index_metadata", "maintenance"] } ]
}'
appel PUT "/_security/user/spark_es" -d "{\"password\":\"${SPARK_ES_PASSWORD}\",\"roles\":[\"spark_writer\"],\"full_name\":\"Compte Spark labsecu\"}"

appel PUT "/_index_template/securite-evenements" --data-binary @/setup/template-evenements.json
appel PUT "/_index_template/securite-alertes"    --data-binary @/setup/template-alertes.json

echo "Elasticsearch pret."
