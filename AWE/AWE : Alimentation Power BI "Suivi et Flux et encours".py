# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 06/01/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------


# Pour importer des fonctions spécifique aux lignes de commande
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
from pyspark.sql.functions import when, datediff, dayofmonth, substring, month, year, lit, desc, concat_ws, regexp_replace, dense_rank, row_number, lag, max, lead, to_date, countDistinct, current_date, date_format, concat_ws, col, lpad,sum, col, trim, round
from pyspark.sql.types import StructType, StructField, DoubleType,LongType
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import calendar
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier


# COMMAND ----------


# Chemin vers le répertoire contenant les fichiers
chemin = "/FileStore/tables/AWE/"

# Liste de tout les fichiers qui existente dans le répertoire ( sans les charger )
tous_les_fichiers = dbutils.fs.ls(chemin)
tous_les_fichiers

# COMMAND ----------

# Obtenir le mois et l'année actuels

mois_actuel = datetime.now().month
print(mois_actuel)
annee_actuelle = datetime.now().year
print(annee_actuelle)

# Calculer le mois et l'année du mois précédent

mois = mois_actuel - 1 if mois_actuel > 1 else 12
print(mois)
annee = annee_actuelle - 1 if mois == 12 else annee_actuelle
print(annee)

# COMMAND ----------



fichiers_mois_12 = []
max_mois_annee_en_cours = 0

for fichier in tous_les_fichiers:

    nom_fichier = fichier[1]
    #print(nom_fichier)

    # Extraire l'année du nom de fichier

    mois_fichier = int(nom_fichier.split("_")[1])
    #print(mois_fichier)

    # Extraire le mois du nom de fichier

    annee_fichier = int(nom_fichier.split("_")[2][0:4])
    #print(annee_fichier)

    #########
            # On récupère tout les noms des fichiers de décembre 
    #########


    if annee_fichier < annee_actuelle and mois_fichier == 12 :
        fichiers_mois_12.append(nom_fichier)

    #########
            # On récupère le mois de l'année en cours
    #########


    # Je veux connaitre le mois le plus grand du mois en cours

    if annee_fichier == annee_actuelle and mois_fichier > max_mois_annee_en_cours:
                max_mois_annee_en_cours = mois_fichier
                





# COMMAND ----------

# VERIF 1

print(fichiers_mois_12)

# VERIF 2

print("FLUX_"+str(max_mois_annee_en_cours)+"_"+str(annee_actuelle)+".csv")

# COMMAND ----------

# SI le mois précédent est un mois de décembre alors on prend seulement les mois de décembre 

file_type = "csv"
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

import_fichiers = []

for fichier_mois_12 in fichiers_mois_12:
    
    file_location = f"/FileStore/tables/AWE/{fichier_mois_12}"
    print(file_location)

    import_fichier = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location) \
        .withColumn("periode",lit(f"{fichier_mois_12}")) \
        .withColumn('mois_fichier', lit(12)) \
        .withColumn("annee_fichier",lit(f"{fichiers_mois_12}".split("_")[2][0:4])) \
        .withColumn("mois_actuel",lit(f"{mois_actuel}")) \
        .withColumn("annee_actuelle",lit(f"{annee_actuelle}"))
        
    import_fichiers.append(import_fichier)

# on prend le mois max du mois en cours sauf dans le cas ou le mois précédent du mois actuel est décembre

if mois_actuel != 1:
    
    for PM_ou_FLUX in ["PM","FLUX"]:
        
        file_location = f"/FileStore/tables/AWE/{PM_ou_FLUX}_{max_mois_annee_en_cours}_{annee_actuelle}.csv"
        print(file_location)
  
        import_fichier2 = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .load(file_location) \
            .withColumn("periode",lit(f"{PM_ou_FLUX}_{max_mois_annee_en_cours}_{annee_actuelle}.csv")) \
            .withColumn("mois_fichier",lit(f"{max_mois_annee_en_cours}")) \
            .withColumn("annee_fichier",lit(f"{annee_actuelle}")) \
            .withColumn("mois_actuel",lit(f"{mois_actuel}")) \
            .withColumn("annee_actuelle",lit(f"{annee_actuelle}"))
        
        import_fichiers.append(import_fichier2)

# COMMAND ----------


df = import_fichiers[0]

for fich in import_fichiers[1:]:
    df=  df.unionByName(fich,allowMissingColumns=True)

# COMMAND ----------

# Remplacer le séparateur décimal par "."

df = df.withColumn("PM_FIN", regexp_replace(col("PM_FIN"), ",", ".")) \
       .withColumn("PM_FIN", regexp_replace(col("PM_FIN"), " ", "")
       .cast("long"))

# COMMAND ----------

# supprimer les doublons lignes 

df = df.dropDuplicates()

# COMMAND ----------

# VERIF 3 : avec le fichier excel

(df.groupby("annee_fichier", "mois_fichier")
 .agg(F.sum(F.col("PM_FIN")).alias("PM_FIN"))
).display()

# COMMAND ----------

# VERIF 4

df.select(col("periode")).distinct().display()

# COMMAND ----------

# VERIF 5

df.select(col("TYPE_SUPPORT")).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC On run un nettoyage des données communs pour l'alimentation de la base épargne et pour le power BI pour éviter des erreurs

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETAPE 2 : Nettoyage des données AWE

# COMMAND ----------

# MAGIC %run /Shared/AWE/fonction  

# COMMAND ----------


# Charger ou créer un DataFrame
# Supposons que vous ayez déjà un DataFrame appelé df

# Appeler la fonction sur le DataFrame
processed_df = process_dataframe(df)

# Afficher le résultat
display(processed_df)



# COMMAND ----------

# VERIF 3 : avec le fichier excel

(df.groupby("annee_fichier", "mois_fichier")
 .agg(F.sum(F.col("PM_FIN")).alias("PM_FIN"))
).display()

# COMMAND ----------

# VERIF 6

processed_df.select(col("RESEAU")).distinct().display()

# COMMAND ----------




### Pour intégration des AWE dans le power BI "Suivi et Flux et encours"

Base_AWE_power_bi = ( processed_df
                            .withColumn("DATE_EFFET_FLUX", to_date(col("DATE_EFFET_FLUX"), "dd/MM/yyyy"))  
                            .withColumn("DATE_EFFET_CONTRAT", to_date(col("DATE_EFFET_CONTRAT"), "dd/MM/yyyy")) 

                            .withColumnRenamed("NUMERO_CONTRAT", "N°Contrat")   
                            .withColumnRenamed("PM_FIN", "Encours")
                            .withColumnRenamed("CODE_PORTEFEUILLE", "code_apporteur")                            
                            .withColumnRenamed("TYPE_TRANSACTION", "groupe")
                            .withColumnRenamed( "RESEAU","LB_RESEAU")
                            .withColumnRenamed("DATE_EFFET_CONTRAT", "Date_de_souscription")
                            .withColumnRenamed("NOM_APPORTEUR", "apporteur")
                            .withColumnRenamed("NOM_PRODUIT", "libprod")

                            .withColumn("MONTANT_BRUT", col("MONTANT_BRUT")/1000)
                            .withColumn("Encours", col("Encours")/1000)

                            .withColumn("DATE_VALEUR",when( col(("DATE_EFFET_FLUX")).isNull(), col("Date_de_souscription") ).otherwise(col(("DATE_EFFET_FLUX")))) 

# arbitrage 

                            .withColumn("nature", when((col("groupe")=="Arbitrage") & (col("MONTANT_BRUT") < 0) & (col("ACTE_GESTION")!="Opération sur titres"), "Arbitrage sortant")
                                                 .when((col("groupe")=="Arbitrage") & (col("MONTANT_BRUT") > 0) & (col("ACTE_GESTION")!="Opération sur titres"), "Arbitrage entrant")
                                                 .otherwise(col("ACTE_GESTION")))
                            
                            # On met les montants en positifs pour les rachats partiels et les rachats total et l'arbitrage sortant

                            .withColumn("MONTANT_BRUT", when((col("nature")=="Arbitrage sortant"),col("MONTANT_BRUT")*-1).otherwise(col("MONTANT_BRUT"))) 
                            .withColumn("MONTANT_BRUT", when(col("nature").isin(["Rachat partiel","Rachat total"]),col("MONTANT_BRUT")*-1).otherwise(col("MONTANT_BRUT"))) 
                            
                  
                            # on veut un seul filtre annee sur les encours et les flux dans

                            .withColumn("Annee_valeur",col("annee_fichier"))
                            # .withColumn("Mois_valeur",when((col("nature")=="Encours")&(),lit(f"{mois_fichier}")).otherwise(month(col("DATE_VALEUR"))))

                            .withColumn("Mois_valeur",when((col("nature")=="Encours") & 
                                                           (col("annee_fichier") < col("annee_actuelle")),lit(12))
                                                      .when((col("nature")!="Encours"),month(col("DATE_VALEUR")))
                                                      .otherwise(col("mois_actuel")))
)





# COMMAND ----------

# MAGIC %md
# MAGIC # RAJOUT DE LA REGION

# COMMAND ----------

# MAGIC %run /Shared/RDU/RDU

# COMMAND ----------


# On récupère les REGION

RDU= RDU.select(col("CODE_PORTEFEUILLE").alias("code_apporteur"),col("REGION"))

Base_AWE_power_bi_ac_region = Base_AWE_power_bi.join(RDU,['code_apporteur'],how="left")


# COMMAND ----------

# VERIF 10 : avec le fichier excel

(Base_AWE_power_bi_ac_region.where(col("groupe")=="Prime").groupby("annee_fichier", "mois_fichier")
 .agg(F.sum(F.col("MONTANT_BRUT")).alias("FLUX"))
).display()

# COMMAND ----------



# COMMAND ----------

Base_AWE_power_bi_ac_region.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").saveAsTable("powerbi.details_AWE")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# VERIF 7

Base_AWE_power_bi.select(col("TYPE_SUPPORT")).distinct().display()

# COMMAND ----------

# VERIF 8

Base_AWE_power_bi.select(col("Mois_valeur")).distinct().display()

# COMMAND ----------

# VERIF 9

Base_AWE_power_bi.count()

# COMMAND ----------



(Base_AWE_power_bi.groupby("Annee_valeur","TYPE_SUPPORT","nature")
            .agg(
                F.sum(F.col("MONTANT_BRUT")).alias("MONTANT_BRUT"),
                
            )
            

        ).display()

# COMMAND ----------


#telecharger_dataframe(Base_AWE)

# COMMAND ----------


