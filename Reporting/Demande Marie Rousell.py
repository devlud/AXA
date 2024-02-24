# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic Ravenach")
print("Date de création : 27/01/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

annee="2022" #Indiquer l'annee 
mois="12"    #Indiquer le mois 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import when, datediff, dayofmonth, substring, month, year, lit, desc, concat_ws, regexp_replace, dense_rank, row_number, lag, max, lead, to_date, countDistinct, current_date, date_format, concat_ws
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import calendar
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier

# COMMAND ----------

  
 DATA=( spark.read.format('DELTA').load('/mnt/base_sauv/Base_Epargne_Finale.DELTA').where('func_month="' + annee + mois + '"')
.filter(F.col('CODE_PORTEFEUILLE').isin([
"0120165884"])       )

.groupBy(F.col("TOP_ONE"),F.col("CODE_PORTEFEUILLE"),F.col("TYPE_SUPPORT"),F.col("APPORTEUR"),F.col("SYSTEME")  )
 .agg(
F.sum(F.col("Collecte_Brute_TOTALE")).alias("Collecte_Brute_TOTALE"),
F.sum(F.col("Prestation_TOTALE")).alias("Prestation_TOTALE"),
F.sum(F.col("Arbitrage_Entrant")).alias("Arbitrage_Entrant"),
F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrage_Sortant"),
F.sum(F.col("PM_FIN")).alias("PM_FIN"))



                                 ) 


# COMMAND ----------

telecharger_dataframe(DATA)
