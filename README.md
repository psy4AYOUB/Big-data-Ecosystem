# Big Data Ecosystem — labsecu : collecte et analyse des logs de sécurité

Projet ISET Nabeul · RSI 3 · 2026-2027.
Toute la chaîne d'analyse tourne dans **Docker** sur SRV-LINUX. Le contrôleur de domaine Windows (SRV-AD) et la machine d'attaque (KALI) restent des machines virtuelles.

```
Nginx / SSH / UFW ──► Filebeat ─┐
                                ├─► Logstash ─► Kafka ─► Spark Streaming ─┬─► Cassandra
Windows AD (SRV-AD) ► Winlogbeat┘   (format     (topic     (4 règles)     └─► Elasticsearch ─► Kibana
                                     commun)     security-logs)
```

## Les machines

| Machine | Adresse | Rôle |
|---|---|---|
| SRV-AD | 192.168.50.10 | Windows Server 2022, Active Directory + DNS, **Winlogbeat** (hors Docker) |
| SRV-LINUX | 192.168.50.20 | Ubuntu 24.04 + Docker : toute la chaîne ci-dessous |
| KALI | 192.168.50.100 | Machine d'attaque pour les tests (autorisés, dans le labo) |

## Les conteneurs

| Conteneur | Rôle | Port publié |
|---|---|---|
| `labsecu-nginx` | Site Web de test, écrit les logs d'accès | 80 |
| `labsecu-filebeat` | Lit les logs Nginx + `auth.log` (SSH) et `ufw.log` de SRV-LINUX | — |
| `labsecu-logstash` | Met tous les logs au même format, envoie à Kafka | 5045 (Winlogbeat) |
| `labsecu-kafka` | File d'attente, topic `security-logs` (3 partitions) | — |
| `labsecu-spark` | Spark Structured Streaming : enrichit, détecte, écrit | — |
| `labsecu-cassandra` | Archive : keyspace `securite_logs`, 5 tables | — |
| `labsecu-elasticsearch` | Recherche : index `securite-evenements-*` et `securite-alertes-*` | 127.0.0.1:9200 |
| `labsecu-kibana` | Tableau de bord | 5601 |
| `*-init`, `*-setup` | Création du topic, du schéma, des comptes et des vues (une seule fois) | — |

## Les 4 règles de détection (`spark/app/regles_detection.py`)

| Règle | Condition | Gravité |
|---|---|---|
| R1 force brute AD | ≥ 5 échecs (4625/4771) sur un même compte en 2 min | high |
| R2 force brute SSH | ≥ 5 échecs SSH depuis une même IP en 2 min | high |
| R3 erreurs HTTP | ≥ 20 erreurs 4xx/5xx depuis une même IP en 1 min | medium |
| R4 succès après échecs | ≥ 3 échecs puis un succès sur un même compte en 5 min | critical |

## Installation sur SRV-LINUX

Prérequis : Docker Engine + plugin Compose, 8 Go de RAM pour la VM (12 Go conseillés), accès Internet (images + connecteurs Spark).

```bash
git clone https://github.com/psy4AYOUB/Big-data-Ecosystem.git
cd Big-data-Ecosystem
cp .env.example .env
nano .env                          # changer les mots de passe
./scripts/preparer-hote.sh         # une seule fois : arrête les anciens services, règle vm.max_map_count
docker compose up -d --build       # 3 à 5 minutes au premier lancement
./scripts/verifier.sh
```

Kibana : **http://192.168.50.20:5601** (utilisateur `elastic`, mot de passe de `.env`).
Les vues « Événements de sécurité » et « Alertes de sécurité » sont créées automatiquement.

### Tableau de bord Kibana

Pour retrouver le tableau de bord « Supervision sécurité - labsecu » :
1. Dans l'ancienne installation : *Stack Management → Objets enregistrés*, cocher le tableau de bord, **Exporter** avec « inclure les objets liés ».
2. Copier le fichier obtenu dans `kibana/dashboard.ndjson`.
3. `docker compose up -d --force-recreate kibana-setup` : il est importé automatiquement.

### SRV-AD (Windows)

Winlogbeat reste installé sur SRV-AD. Sa configuration est dans `winlogbeat/winlogbeat.yml` : il envoie à `srv-linux.labsecu.lan:5045`, le port publié par le conteneur Logstash. Rien à changer si Docker tourne sur SRV-LINUX.

## Tester depuis KALI

```bash
cd ~/labsecu; read -rs 'PW?Mot de passe des comptes de test : '; echo
gobuster dir -u http://srv-linux -w pages_test.txt -t 10 -q --no-error                 # R3
hydra -l testeur_demo -P mdp_fictifs.txt -t 2 -I ssh://192.168.50.20                    # R2
netexec smb 192.168.50.10 -d labsecu.lan -u charlie.lock -p mdp_fictifs.txt             # R1
head -n 3 mdp_fictifs.txt > mdp_3.txt
netexec smb 192.168.50.10 -d labsecu.lan -u alice.test -p mdp_3.txt
sleep 5; netexec smb 192.168.50.10 -d labsecu.lan -u alice.test -p "$PW"                # R4
```

Puis, 1 à 2 minutes plus tard :

```bash
docker logs labsecu-spark 2>&1 | grep ALERTE | tail
```

Et dans Kibana : *Discover → Alertes de sécurité*, filtre `regle : "R4_succes_apres_echecs"`.
Après le test, sur SRV-AD : `Unlock-ADAccount charlie.lock`.

## Commandes utiles

| Commande | À quoi elle sert |
|---|---|
| `docker compose ps` | État des conteneurs |
| `docker compose logs -f spark` | Journal du job Spark en direct |
| `docker exec labsecu-cassandra cqlsh -k securite_logs -e "SELECT * FROM alertes_par_regle LIMIT 10;"` | Alertes dans Cassandra |
| `docker exec labsecu-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic security-logs --max-messages 3` | Voir 3 messages du topic |
| `docker compose down` | Tout arrêter (les données restent dans les volumes) |
| `docker compose down -v` | Tout arrêter **et effacer** les données |

## Arborescence

```
docker-compose.yml      orchestration de tous les conteneurs
.env.example            versions et mots de passe (copier en .env)
nginx/                  site Web de test
filebeat/               collecte des logs Linux
winlogbeat/             collecte Windows (à copier sur SRV-AD)
logstash/pipeline/      entrées, normalisation Linux/Windows, sortie Kafka
cassandra/schema.cql    keyspace et 5 tables
elasticsearch/          modèles d'index + comptes kibana_system / spark_es
kibana/                 vues de données (+ dashboard.ndjson optionnel)
spark/                  image et code du job (traitement, règles, écriture)
scripts/                préparation de l'hôte et vérification
```

## Sécurité et limites

- Comptes et mots de passe **fictifs** ; le fichier `.env` n'est jamais versionné.
- Elasticsearch n'est joignable que depuis SRV-LINUX (127.0.0.1) ; Kafka et Cassandra seulement sur le réseau Docker interne.
- HTTPS désactivé sur Elasticsearch (laboratoire) ; Kafka et Cassandra sans authentification.
- Fenêtres de temps fixes : une attaque à cheval sur deux fenêtres peut être coupée en deux.
- Docker publie ses ports avant UFW : le filtrage par adresse de 5045 et 5601 dépend des règles iptables de Docker.
