"""
=====================================================================
 Regles de detection - Projet labsecu (Etape 6.3)
 Chaque regle : filtre -> regroupement (cle + fenetre) -> seuil -> alerte
=====================================================================
"""
from pyspark.sql import functions as F

# ---------------------------------------------------------------------
# 0. Seuils et fenetres (modifiables)
# ---------------------------------------------------------------------
SEUIL_R1, FENETRE_R1 = 5, "2 minutes"    # force brute AD (par utilisateur)
SEUIL_R2, FENETRE_R2 = 5, "2 minutes"    # force brute SSH (par IP)
SEUIL_R3, FENETRE_R3 = 20, "1 minute"    # rafale d'erreurs HTTP (par IP)
SEUIL_R4, FENETRE_R4 = 3, "5 minutes"    # succes apres >= 3 echecs (par utilisateur)
WATERMARK = "3 minutes"                  # tolerance pour les evenements en retard


# ---------------------------------------------------------------------
# 1. Mise en forme commune d'une alerte
# ---------------------------------------------------------------------
def formater_alerte(df, regle, severite, description):
    return (df
        .withColumn("regle", F.lit(regle))
        .withColumn("severite", F.lit(severite))
        .withColumn("fenetre_debut", F.col("window.start"))
        .withColumn("fenetre_fin", F.col("window.end"))
        .withColumn("alert_time", F.col("window.start"))          # stable : la meme alerte garde la meme cle
        .withColumn("jour", F.to_date("alert_time"))
        .withColumn("alert_id", F.sha2(F.concat_ws("|",
                    F.lit(regle),
                    F.coalesce(F.col("utilisateur"), F.lit("-")),
                    F.coalesce(F.col("adresse_ip"), F.lit("-")),
                    F.col("window.start").cast("string")), 256))
        .withColumn("description", description)
        .select("jour", "alert_time", "alert_id", "regle", "severite", "host",
                "utilisateur", "adresse_ip", "nb_evenements",
                "fenetre_debut", "fenetre_fin", "description"))


# ---------------------------------------------------------------------
# R1 : force brute Active Directory (>= 5 echecs pour un compte en 2 min)
# ---------------------------------------------------------------------
def regle_r1_force_brute_ad(ev):
    t = F.col("type_evenement")
    agregat = (ev
        .filter(t.isin("auth_echec", "kerberos_preauth_echec") & F.col("utilisateur").isNotNull())
        .groupBy(F.window("event_time", FENETRE_R1), "utilisateur")
        .agg(F.sum(F.when(t == "auth_echec", 1).otherwise(0)).alias("nb_4625"),
             F.sum(F.when(t == "kerberos_preauth_echec", 1).otherwise(0)).alias("nb_4771"),
             F.first("host").alias("host"),
             F.first("adresse_ip", ignorenulls=True).alias("adresse_ip"),
             F.max("event_time").alias("derniere_occurrence"))
        # max et non somme : une meme tentative peut produire 4625 ET 4771
        .withColumn("nb_evenements", F.greatest("nb_4625", "nb_4771").cast("int"))
        .filter(F.col("nb_evenements") >= SEUIL_R1))
    description = F.format_string(
        "Force brute AD : %d echecs d'authentification pour le compte '%s' en 2 minutes (derniere tentative %s)",
        F.col("nb_evenements"), F.col("utilisateur"), F.date_format("derniere_occurrence", "HH:mm:ss"))
    return formater_alerte(agregat, "R1_force_brute_ad", "high", description)


# ---------------------------------------------------------------------
# R2 : force brute SSH (>= 5 echecs depuis une meme IP en 2 min)
# ---------------------------------------------------------------------
def regle_r2_force_brute_ssh(ev):
    agregat = (ev
        .filter((F.col("type_evenement") == "ssh_echec") & F.col("adresse_ip").isNotNull())
        .groupBy(F.window("event_time", FENETRE_R2), "adresse_ip")
        .agg(F.count("*").cast("int").alias("nb_evenements"),
             F.first("host").alias("host"),
             F.first("utilisateur", ignorenulls=True).alias("utilisateur"),
             F.approx_count_distinct("utilisateur").alias("nb_comptes"),
             F.max("event_time").alias("derniere_occurrence"))
        .filter(F.col("nb_evenements") >= SEUIL_R2))
    description = F.format_string(
        "Force brute SSH : %d echecs depuis l'IP %s en 2 minutes (%d compte(s) vise(s)), derniere tentative %s",
        F.col("nb_evenements"), F.col("adresse_ip"), F.col("nb_comptes"),
        F.date_format("derniere_occurrence", "HH:mm:ss"))
    return formater_alerte(agregat, "R2_force_brute_ssh", "high", description)


# ---------------------------------------------------------------------
# R3 : rafale d'erreurs HTTP (>= 20 erreurs 4xx/5xx depuis une IP en 1 min)
# ---------------------------------------------------------------------
def regle_r3_erreurs_http(ev):
    agregat = (ev
        .filter(F.col("type_evenement").isin("http_erreur_4xx", "http_erreur_5xx") & F.col("adresse_ip").isNotNull())
        .groupBy(F.window("event_time", FENETRE_R3), "adresse_ip")
        .agg(F.count("*").cast("int").alias("nb_evenements"),
             F.first("host").alias("host"),
             F.lit(None).cast("string").alias("utilisateur"),
             F.max("event_time").alias("derniere_occurrence"))
        .filter(F.col("nb_evenements") >= SEUIL_R3))
    description = F.format_string(
        "Hausse inhabituelle d'erreurs HTTP : %d erreurs depuis l'IP %s en 1 minute (scan ou enumeration possible)",
        F.col("nb_evenements"), F.col("adresse_ip"))
    return formater_alerte(agregat, "R3_erreurs_http", "medium", description)


# ---------------------------------------------------------------------
# R4 : connexion reussie apres plusieurs echecs (>= 3 echecs puis succes en 5 min)
# ---------------------------------------------------------------------
def regle_r4_succes_apres_echecs(ev):
    t = F.col("type_evenement")
    echec = t.isin("auth_echec", "ssh_echec")
    succes = t.isin("auth_succes", "ssh_succes")
    agregat = (ev
        .filter((echec | succes) & F.col("utilisateur").isNotNull())
        .groupBy(F.window("event_time", FENETRE_R4), "utilisateur")
        .agg(F.sum(F.when(echec, 1).otherwise(0)).alias("nb_echecs"),
             F.sum(F.when(succes, 1).otherwise(0)).alias("nb_succes"),
             F.min(F.when(echec, F.col("event_time"))).alias("premier_echec"),
             F.max(F.when(succes, F.col("event_time"))).alias("dernier_succes"),
             F.first("host").alias("host"),
             F.first("adresse_ip", ignorenulls=True).alias("adresse_ip"))
        # au moins 3 echecs, au moins 1 succes, et le succes APRES le premier echec
        .filter((F.col("nb_echecs") >= SEUIL_R4) & (F.col("nb_succes") >= 1) &
                (F.col("dernier_succes") > F.col("premier_echec")))
        .withColumn("nb_evenements", (F.col("nb_echecs") + F.col("nb_succes")).cast("int")))
    description = F.format_string(
        "Connexion reussie apres %d echecs pour le compte '%s' (succes a %s) : mot de passe possiblement devine",
        F.col("nb_echecs"), F.col("utilisateur"), F.date_format("dernier_succes", "HH:mm:ss"))
    return formater_alerte(agregat, "R4_succes_apres_echecs", "critical", description)


# Liste des regles : (nom de la requete, fonction)
REGLES = [
    ("r1_force_brute_ad", regle_r1_force_brute_ad),
    ("r2_force_brute_ssh", regle_r2_force_brute_ssh),
    ("r3_erreurs_http", regle_r3_erreurs_http),
    ("r4_succes_apres_echecs", regle_r4_succes_apres_echecs),
]
