#!/bin/bash
# A lancer UNE FOIS sur SRV-LINUX avant le premier "docker compose up".
#  1. arrete les services installes "a la main" (ils prendraient les memes ports)
#  2. regle vm.max_map_count pour Elasticsearch
#  3. ouvre le pare-feu comme avant (5045 pour SRV-AD, 5601 pour le PC)
set -e
echo "1) Arret des anciens services (non Docker)..."
for s in labsecu-spark kibana elasticsearch logstash filebeat kafka cassandra nginx; do
  if systemctl list-unit-files | grep -q "^$s.service"; then
    sudo systemctl disable --now "$s" && echo "   $s arrete et desactive"
  fi
done

echo "2) vm.max_map_count pour Elasticsearch..."
echo "vm.max_map_count=262144" | sudo tee /etc/sysctl.d/99-elasticsearch.conf >/dev/null
sudo sysctl --system >/dev/null
sysctl vm.max_map_count

echo "3) Pare-feu UFW..."
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow from 192.168.50.10 to any port 5045 proto tcp
sudo ufw allow from 192.168.50.1 to any port 5601 proto tcp
echo "Attention : Docker publie ses ports avant UFW (regles iptables propres a Docker)."
echo "Pret. Lancez maintenant : docker compose up -d --build"
