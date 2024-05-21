# Databricks notebook source
print("=======================================================================================================================================")
print("")

print("Auteur du Programme : AGKAYA Ibrahim")
print("Date de création : 27/03/2023")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

annee="2023" #Indiquer l'annee 
mois="12"   #Indiquer le mois 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import when, datediff, dayofmonth, substring, month, year, lit, desc, concat_ws, regexp_replace, dense_rank, row_number, lag, max, lead, to_date, countDistinct, current_date, date_format, concat_ws,col,collect_set, array_contains,size
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import calendar
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier

# COMMAND ----------

# MAGIC %run /Shared/FCA/FCA 

# COMMAND ----------


 DATA=( spark.read.format('DELTA').load('/mnt/base_sauv/Base_Epargne_Finale.DELTA').where('func_month="' + annee + mois + '"')
       
.withColumn("SEGPAT", F.when(F.col("SEGPAT").isNull(),F.lit("ND"))
.otherwise(F.col("SEGPAT")))

.withColumn("RESEAU", F.when(F.col("RESEAU").isNull(),F.lit("ND"))
.otherwise(F.col("RESEAU")))
       

.withColumn("SEGPAT", F.when(F.col("NOM_PRODUIT").isin(["FONDS DE PENSION. FAR","PER AMADEO","MA RETRAITE","PER CORALIS"]),F.lit("PER"))
.otherwise(F.col("SEGPAT")))

.withColumn("TYPE_SUPPORT", F.when(F.col("TYPE_SUPPORT").isin(["EUROCROISSANCE","EURO"]),F.lit("EURO"))
.otherwise(F.lit("UC")))


# AGE DU CLIENT

.withColumn("DATE_NAISSANCE", to_date(col("DATE_NAISSANCE"), "dd/MM/yyyy")) 
.withColumn("age_client", lit(annee) -  year(col("DATE_NAISSANCE")))

# Nouveau contrat dans l'année
                  
.withColumn("DATE_EFFET", to_date(col("DATE_EFFET"), "dd/MM/yyyy"))     
.withColumn("TOP_AN",F.when(( F.col("CNT")==1)  & (year(col("DATE_EFFET"))==annee),1).otherwise(0) )

# Anciennete contrat

.withColumn('ANCIENNETE_CONTRAT', datediff(lit(annee), col('DATE_EFFET'))/365.25)




.withColumn("SEG", F.when( (   (   (F.col("SEGPAT").isin(["NON SEGMENTE","ND"]) )            )
 &   (F.col("RESEAU").isin(["A2P","AEP","AGA","AXA PARTENAIRE","DIVERS AXA","DOM-TOM","GUICHET NATIO","ND"])))          ,F.lit("GRAND PUBLIC"))

.when(  ( (F.col("SEGPAT").isin(["NON SEGMENTE","ND"]) ) &   (F.col("RESEAU").isin(["PARTENAIRE BANCAIRE","PARTENAIRE DISTRIBUTION",
                                                                                    "GESTION PRIVEE DIRECTE"]))),
F.lit("GESTION PRIVEE"))

.when(  ( (F.col("SEGPAT").isin(["NON SEGMENTE","ND"]) ) &   (F.col("RESEAU").isin(["CGPI","COURTIERS GENERALISTES"]))),F.lit("AFFLUENT"))

.otherwise(F.col("SEGPAT")))

.withColumn("Collecte_Nette",F.col("Collecte_Brute_TOTALE") - F.col("Prestation_TOTALE") + F.col("Arbitrage_Entrant") - 
F.col("Arbitrage_Sortant") )

 )



# COMMAND ----------

######## 
        # Calcul si mono ou multisupport
########

# Définir une fenêtre glissante pour collecter toutes les valeurs uniques de "type_de_support" pour chaque contrat

windowSpec = Window.partitionBy("NOM_PRODUIT")

# Collecter toutes les valeurs uniques de "TYPE_SUPPORT" pour chaque contrat dans une nouvelle colonne "valeurs_uniques"

DATA = DATA.withColumn("valeurs_uniques", collect_set("TYPE_SUPPORT").over(windowSpec))

# Créer une nouvelle colonne "monosupport" EURO qui indique si chaque contrat a un seul support

DATA = DATA.withColumn("TYPE_SUPPORT_2",
                   when((size(DATA.valeurs_uniques) == 1) & array_contains(DATA.valeurs_uniques, "EURO"), lit("MONOSUPPORT_EURO"))
                   .when((size(DATA.valeurs_uniques) == 1) & array_contains(DATA.valeurs_uniques, "UC"), lit("MONOSUPPORT_UC"))
                   .when((size(DATA.valeurs_uniques) > 1), lit("MULTISUPPORT"))
                   .otherwise(lit("Autres"))
                  )

# COMMAND ----------

# VERIF

DATA.select(col("TYPE_SUPPORT_2")).distinct().display()

# COMMAND ----------

######## 
        # Nombre de client 
########


# Si un client a plusieur SEGPAT => on créé une hiérarchie dans la décomposition de SEG
# s'il apparait dans 1 et dans 2 alors il est dans 1, s'il apparait dans 2 et dans 4 alors il est dans 2,...

DATA = DATA.withColumn("CLIENT_ORDER",
                   when(col("SEG")=="PER", lit(1))
                   .when(col("SEG")=="GESTION PRIVEE", lit(2))
                   .when(col("SEG")=="AFFLUENT", lit(3))
                   .when(col("SEG")=="GRAND PUBLIC", lit(4))
                   .otherwise(lit(5))
                  )

# Créer une fenêtre glissante qui trie les données par client
windowSpec = Window.orderBy(asc("SEG"))

# Ajouter une colonne "first_appearance" qui indique si un client apparaît pour la première fois ou non
DATA = DATA.withColumn("NB_CLIENT", when(col("NUMERO_ABONNE") != lag(col("NUMERO_ABONNE")).over(windowSpec), lit(1)).otherwise(lit(0)))



# COMMAND ----------

######## 
        # On aggrège
########


DATA=(DATA
    
.groupBy(F.col("CANAL"),F.col("RESEAU"),F.col("SEG"),F.col("NOM_PRODUIT"),F.col("TOP_RETRAITE"),F.col("TYPE_SUPPORT"),F.col("SYSTEME"),F.col("TYPE_SUPPORT_2"))
 .agg(
    

F.sum(F.col("Collecte_Brute_TOTALE")).alias("Collecte_Brute_TOTALE"),
F.sum(F.col("Versement_Initial")).alias("Versement_Initial"),  
F.sum(F.col("Versement_Complementaire")).alias("Versement_Complementaire"),  
F.sum(F.col("Prelevement_Automatique")).alias("Prelevement_Automatique"),
F.sum(F.col("Transfert_Entrant")).alias("Transfert_Entrant"),


F.sum(F.col("Prestation_TOTALE")).alias("Prestation_TOTALE"),                                    
F.sum(F.col("Rachat")).alias("Rachat"),
F.sum(F.col("Sinistre")).alias("Sinistre"), 
F.sum(F.col("Echu")).alias("Echu"), 
F.sum(F.col("Transfert_Sortant")).alias("Transfert_Sortant"),
F.sum(F.col("PM_FIN")).alias("PM_FIN"),
F.sum(F.col("Collecte_Nette")).alias("Collecte_Nette"),

F.sum(F.col("TOP_AN")).alias("TOP_AN"),
F.sum(F.col("NB_CLIENT")).alias("NB_CLIENT")


 )
 )

# COMMAND ----------



# COMMAND ----------

 DATA=( spark.read.format('DELTA').load('/mnt/base_sauv/Base_Epargne_Finale.DELTA').where('func_month="' + annee + mois + '"'))
       
# Définir une fenêtre glissante pour collecter toutes les valeurs uniques de "type_de_support" pour chaque produit
windowSpec = Window.partitionBy("NOM_PRODUIT")

# Collecter toutes les valeurs uniques de "type_de_support" pour chaque produit dans une nouvelle colonne "types_de_support"
DATA = DATA.withColumn("valeurs_uniques", collect_set("TYPE_SUPPORT").over(windowSpec))

DATA.display()

# COMMAND ----------

telecharger_dataframe(DATA)
