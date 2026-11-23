#!/bin/bash
# Configuration de Kibana (executee par le conteneur kibana-setup) :
#   - vues de donnees "Evenements de securite" et "Alertes de securite"
#   - import du tableau de bord si kibana/dashboard.ndjson existe
set -uo pipefail
KB="http://kibana:5601"
AUTH="elastic:${ELASTIC_PASSWORD}"

echo "Attente de Kibana (peut prendre 2 a 3 minutes)..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' -u "$AUTH" "$KB/api/status")" = "200" ]; do sleep 10; done

vue() {   # vue ID MOTIF NOM CHAMP_TEMPS
  curl -s -o /dev/null -w "Vue $3 : HTTP %{http_code}\n" -u "$AUTH" -X POST "$KB/api/data_views/data_view" \
    -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
    -d "{\"override\":true,\"data_view\":{\"id\":\"$1\",\"title\":\"$2\",\"name\":\"$3\",\"timeFieldName\":\"$4\"}}"
}
vue "labsecu-evenements" "securite-evenements-*" "Événements de sécurité" "timestamp"
vue "labsecu-alertes"    "securite-alertes-*"    "Alertes de sécurité"    "alert_time"

if [ -f /setup/dashboard.ndjson ]; then
  curl -s -u "$AUTH" -X POST "$KB/api/saved_objects/_import?overwrite=true" \
       -H 'kbn-xsrf: true' -F file=@/setup/dashboard.ndjson
  echo
  echo "Tableau de bord importe."
else
  echo "Pas de kibana/dashboard.ndjson : tableau de bord a creer ou importer (voir README)."
fi
echo "Kibana pret : http://192.168.50.20:5601"
