"""
=====================================================================
 Traitement commun - Projet labsecu
 Lecture du topic Kafka, decodage du JSON produit par Logstash,
 enrichissement (categorie, score de severite, echec) et deduplication.
=====================================================================
"""
import os

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_SERVEURS = os.getenv("KAFKA_SERVEURS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "security-logs")

# Schema commun ecrit par Logstash (90-normalisation-sortie.conf).
# Tout est lu en texte puis converti : un champ mal forme ne bloque pas le flux.
SCHEMA_EVENEMENT = StructType([
    StructField(nom, StringType()) for nom in [
        "timestamp", "source", "host", "systeme", "type_evenement",
        "utilisateur", "adresse_ip", "message", "severite",
        "http_status", "http_method", "http_url",
        "port_source", "port_destination", "protocole", "event_id",
    ]
])

TYPES_ECHEC = ["auth_echec", "kerberos_preauth_echec", "ssh_echec",
               "http_erreur_4xx", "http_erreur_5xx", "firewall_blocage"]

SCORE_SEVERITE = {"info": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}


# ---------------------------------------------------------------------
# 1. Lecture de Kafka
# ---------------------------------------------------------------------
def lire_kafka(spark, depart="earliest"):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVEURS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", depart)   # "earliest" = depuis le debut, "latest" = nouveaux seulement
            .option("maxOffsetsPerTrigger", 2000)  # au plus 2000 messages par micro-batch
            .option("failOnDataLoss", "false")     # ne pas planter si des vieux messages ont expire (retention 72 h)
            .load())


# ---------------------------------------------------------------------
# 2. Decodage + enrichissement
# ---------------------------------------------------------------------
def _score(col):
    expr = F.lit(1)
    for niveau, score in SCORE_SEVERITE.items():
        expr = F.when(col == niveau, F.lit(score)).otherwise(expr)
    return expr


def _categorie(t):
    return (F.when(t.rlike("^(auth|ssh|kerberos|ntlm|connexion|fermeture)"), "authentification")
             .when(t.rlike("^(compte|ajout_groupe)"), "gestion_comptes")
             .when(t.rlike("^(http|nginx)"), "web")
             .when(t.startswith("firewall"), "reseau")
             .when(t == "sudo", "privileges")
             .otherwise("autre"))


def preparer_evenements(df_kafka, watermark="3 minutes", dedupliquer=True):
    """dedupliquer=False pour les regles : Spark 3.5 n'accepte pas deux operateurs
    a etat enchaines (deduplication + agregation) en mode "update"."""
    ev = (df_kafka
          .select(F.from_json(F.col("value").cast("string"), SCHEMA_EVENEMENT).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.to_timestamp("timestamp"))
          .filter(F.col("event_time").isNotNull())
          .withColumn("jour", F.to_date("event_time"))
          .withColumn("severite", F.lower(F.coalesce(F.col("severite"), F.lit("info"))))
          .withColumn("type_evenement", F.coalesce(F.col("type_evenement"), F.lit("autre")))
          .withColumn("score_severite", _score(F.col("severite")).cast("int"))
          .withColumn("categorie", _categorie(F.col("type_evenement")))
          .withColumn("est_echec", F.col("type_evenement").isin(TYPES_ECHEC))
          .withColumn("http_status", F.col("http_status").cast("int"))
          .withColumn("port_source", F.col("port_source").cast("int"))
          .withColumn("port_destination", F.col("port_destination").cast("int"))
          # identifiant stable : meme evenement relu = meme id (pas de doublon)
          .withColumn("event_id_unique", F.sha2(F.concat_ws("|",
                      F.coalesce(F.col("host"), F.lit("-")),
                      F.coalesce(F.col("source"), F.lit("-")),
                      F.col("timestamp"),
                      F.coalesce(F.col("message"), F.lit(""))), 256))
          .withWatermark("event_time", watermark))
    if dedupliquer:
        ev = ev.dropDuplicatesWithinWatermark(["event_id_unique"])
    return ev


COLONNES_EVENEMENT = [
    "host", "jour", "event_time", "event_id_unique", "source", "systeme",
    "type_evenement", "categorie", "utilisateur", "adresse_ip", "message",
    "severite", "score_severite", "est_echec", "event_id",
    "http_status", "http_method", "http_url",
    "port_source", "port_destination", "protocole",
]
