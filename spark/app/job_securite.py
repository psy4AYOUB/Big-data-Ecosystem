"""
=====================================================================
 Job principal - Projet labsecu
   Kafka (security-logs) -> Spark Structured Streaming
     -> Cassandra (securite_logs : 3 tables d'evenements, 2 tables d'alertes)
     -> Elasticsearch (securite-evenements-AAAA.MM.JJ, securite-alertes-AAAA.MM.JJ)
   Les alertes sont aussi ecrites dans le journal : "[ALERTE <regle>] (GRAVITE) description"
=====================================================================
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from traitement_securite import lire_kafka, preparer_evenements, COLONNES_EVENEMENT
from regles_detection import REGLES, WATERMARK

KEYSPACE = "securite_logs"
DEPART = os.getenv("KAFKA_DEPART", "earliest")
CHECKPOINTS = os.getenv("CHECKPOINTS", "/checkpoints")
DECLENCHEUR = os.getenv("DECLENCHEUR", "10 seconds")

ES_OPTIONS = {
    "es.nodes": os.getenv("ES_HOTE", "elasticsearch"),
    "es.port": os.getenv("ES_PORT", "9200"),
    "es.nodes.wan.only": "true",
    "es.net.http.auth.user": os.getenv("ES_UTILISATEUR", "spark_es"),
    "es.net.http.auth.pass": os.getenv("ES_MOT_DE_PASSE", ""),
    "es.batch.write.retry.count": "6",
}

COLONNES_ALERTES = ["jour", "alert_time", "alert_id", "regle", "severite", "host",
                    "utilisateur", "adresse_ip", "nb_evenements",
                    "fenetre_debut", "fenetre_fin", "description"]


def ecrire_cassandra(df, table, colonnes):
    (df.select(*colonnes)
       .write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table)
       .mode("append")
       .save())


def ecrire_es(df, prefixe, colonne_temps, colonne_id):
    (df.withColumn("index_jour", F.date_format(colonne_temps, "yyyy.MM.dd"))
       .write.format("org.elasticsearch.spark.sql")
       .options(**ES_OPTIONS)
       .option("es.resource", prefixe + "-{index_jour}")
       .option("es.mapping.id", colonne_id)
       .option("es.mapping.exclude", "index_jour")
       .option("es.write.operation", "upsert")
       .mode("append")
       .save())


# ---------------------------------------------------------------------
# Evenements : Cassandra (3 tables) + Elasticsearch
# ---------------------------------------------------------------------
def ecrire_evenements(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()
    # une cle de partition Cassandra ne peut pas etre vide
    ecrire_cassandra(batch_df.filter(F.col("host").isNotNull()), "evenements_par_hote", COLONNES_EVENEMENT)
    ecrire_cassandra(batch_df.filter(F.col("source").isNotNull()), "evenements_par_source", COLONNES_EVENEMENT)
    ecrire_cassandra(batch_df, "evenements_par_type", COLONNES_EVENEMENT)
    es_df = batch_df.select(*COLONNES_EVENEMENT).withColumn("timestamp", F.col("event_time"))
    ecrire_es(es_df, "securite-evenements", "event_time", "event_id_unique")
    print(f"[EVENEMENTS] lot {batch_id} : {batch_df.count()} evenements ecrits", flush=True)
    batch_df.unpersist()


# ---------------------------------------------------------------------
# Alertes : Cassandra (2 tables) + Elasticsearch + journal
# ---------------------------------------------------------------------
def ecrivain_alertes(nom_regle):
    def ecrire(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.persist()
        ecrire_cassandra(batch_df, "alertes_par_jour", COLONNES_ALERTES)
        ecrire_cassandra(batch_df, "alertes_par_regle", COLONNES_ALERTES)
        ecrire_es(batch_df.select(*COLONNES_ALERTES), "securite-alertes", "alert_time", "alert_id")
        for a in batch_df.select("severite", "description").collect():
            print(f"[ALERTE {nom_regle}] ({a['severite'].upper()}) {a['description']}", flush=True)
        batch_df.unpersist()
    return ecrire


def main():
    spark = (SparkSession.builder
             .appName("labsecu-detection")
             .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOTE", "cassandra"))
             .config("spark.cassandra.connection.port", os.getenv("CASSANDRA_PORT", "9042"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Flux 1 : evenements dedoublonnes
    evenements = preparer_evenements(lire_kafka(spark, DEPART), WATERMARK, dedupliquer=True)
    (evenements.writeStream
        .queryName("evenements")
        .foreachBatch(ecrire_evenements)
        .option("checkpointLocation", f"{CHECKPOINTS}/evenements")
        .trigger(processingTime=DECLENCHEUR)
        .start())

    # Flux 2 a 5 : une requete par regle (mode update : l'alerte apparait des que le seuil est atteint)
    for nom, regle in REGLES:
        flux = preparer_evenements(lire_kafka(spark, DEPART), WATERMARK, dedupliquer=False)
        (regle(flux).writeStream
            .queryName(nom)
            .outputMode("update")
            .foreachBatch(ecrivain_alertes(nom))
            .option("checkpointLocation", f"{CHECKPOINTS}/{nom}")
            .trigger(processingTime=DECLENCHEUR)
            .start())

    print("[LABSECU] job demarre : 1 flux d'evenements + %d regles" % len(REGLES), flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
