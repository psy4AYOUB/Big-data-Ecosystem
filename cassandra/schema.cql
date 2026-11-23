-- =====================================================================
--  Cassandra : keyspace securite_logs (5 tables, une par question)
--  Execute automatiquement par le conteneur cassandra-init.
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS securite_logs
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE securite_logs;

-- ---------------------------------------------------------------- evenements
-- "Que s'est-il passe sur telle machine tel jour ?"
CREATE TABLE IF NOT EXISTS evenements_par_hote (
  host text, jour date, event_time timestamp, event_id_unique text,
  source text, systeme text, type_evenement text, categorie text,
  utilisateur text, adresse_ip text, message text,
  severite text, score_severite int, est_echec boolean,
  event_id text, http_status int, http_method text, http_url text,
  port_source int, port_destination int, protocole text,
  PRIMARY KEY ((host, jour), event_time, event_id_unique)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id_unique ASC)
  AND default_time_to_live = 2592000;   -- 30 jours

-- "Que dit telle source (nginx_access, auth, windows_security...) tel jour ?"
CREATE TABLE IF NOT EXISTS evenements_par_source (
  source text, jour date, event_time timestamp, event_id_unique text,
  host text, systeme text, type_evenement text, categorie text,
  utilisateur text, adresse_ip text, message text,
  severite text, score_severite int, est_echec boolean,
  event_id text, http_status int, http_method text, http_url text,
  port_source int, port_destination int, protocole text,
  PRIMARY KEY ((source, jour), event_time, event_id_unique)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id_unique ASC)
  AND default_time_to_live = 2592000;

-- "Tous les ssh_echec / auth_echec ... de tel jour"
CREATE TABLE IF NOT EXISTS evenements_par_type (
  type_evenement text, jour date, event_time timestamp, event_id_unique text,
  host text, source text, systeme text, categorie text,
  utilisateur text, adresse_ip text, message text,
  severite text, score_severite int, est_echec boolean,
  event_id text, http_status int, http_method text, http_url text,
  port_source int, port_destination int, protocole text,
  PRIMARY KEY ((type_evenement, jour), event_time, event_id_unique)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id_unique ASC)
  AND default_time_to_live = 2592000;

-- ---------------------------------------------------------------- alertes
-- "Quelles alertes aujourd'hui ?"
CREATE TABLE IF NOT EXISTS alertes_par_jour (
  jour date, alert_time timestamp, alert_id text,
  regle text, severite text, host text, utilisateur text, adresse_ip text,
  nb_evenements int, fenetre_debut timestamp, fenetre_fin timestamp, description text,
  PRIMARY KEY ((jour), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);

-- "Historique d'une regle (R1, R2...)"
CREATE TABLE IF NOT EXISTS alertes_par_regle (
  regle text, alert_time timestamp, alert_id text,
  jour date, severite text, host text, utilisateur text, adresse_ip text,
  nb_evenements int, fenetre_debut timestamp, fenetre_fin timestamp, description text,
  PRIMARY KEY ((regle), alert_time, alert_id)
) WITH CLUSTERING ORDER BY (alert_time DESC, alert_id ASC);
