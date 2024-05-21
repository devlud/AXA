# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 12/01/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

# COMMAND ----------

#from axapy.labs.piper import  DtmcliReader
#DtmcliRead= DtmcliReader()
#DtmcliRead.list_table_doc()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from delta.tables import *

import calendar
annee_N_1= str((int(annee)-1))
jour=str(calendar.monthrange(int(annee), int(mois))[1])
from datetime import date
from datetime import datetime, timedelta, date
test_date = date(int(annee),int(mois),1)

diff=0
for k in [0,1,2,3,4,5,6]:
  if test_date.weekday() == 6:
      ajout = 1
  elif test_date.weekday() == 5:
      ajout = 2
  elif test_date.weekday() == 4:
      ajout = 3
  elif test_date.weekday() == 3:
      ajout = 4
  elif test_date.weekday() == 2:
      ajout = 5
  elif test_date.weekday() == 1:
      ajout = 6
  elif test_date.weekday() == 0:
      ajout = 7

  date_fin = str(test_date + timedelta(days=ajout))


df_client = (spark.read.format('PARQUET').load('/mnt/lake/dtmcli/data/bdtmfichclntrefine/b_dtm_fich_clnt/refine/b_dtm_fich_clnt.PARQUET/func_partitioning_year='+ annee+'/func_partitioning_month='+ annee +'-'+mois+'/func_partitioning_day='+ date_fin +'/')
.withColumn("NUMERO_CLIENT_2",lpad(F.col("NMCLI"), 10, '0'))
.select(F.col("NUMERO_CLIENT_2"),F.col("NMPERS").alias("NUMERO_ABONNE"),
F.col("CDQUALIT").alias("CIVILITE"),F.concat_ws(" ",F.col("NOM"),F.col("PRENOM")).alias("NOM_PRENOM_SOUSCRIPTEUR"),F.col("DTNAISCL").alias("DATE_NAISSANCE"),F.col("ADRESSE"),
F.col("CODE_POSTAL"),F.col("VILLE"),F.col("PAYS"),F.col("EMAIl"),F.col("CSP_BRUTE").alias("CODE_CSP"),F.col("SEGPAT"),F.col("SOUS_SEGPAT"),F.col("RAISON_SOCIALE"),F.col("CIVILITE_PM"),F.col("TYPE"))
.withColumn("NOM_PRENOM_SOUSCRIPTEUR",F.when(F.col("NOM_PRENOM_SOUSCRIPTEUR").isNull(),F.col("RAISON_SOCIALE")).otherwise(F.col("NOM_PRENOM_SOUSCRIPTEUR"))        )  
.drop("RAISON_SOCIALE")        
).dropDuplicates(["NUMERO_CLIENT_2"])



df_contrat = (spark.read.format('PARQUET').load('/mnt/lake/dtmcli/data/bdtmcntgenrefine/b_dtm_cnt_gen/refine/b_dtm_cnt_gen.PARQUET/func_partitioning_year='+ annee+'/func_partitioning_month='+ annee +'-'+mois+'/func_partitioning_day='+ date_fin +'/').select(F.col("NMCLI"),lpad(F.col("NMCNT"), 16, '0').alias("NUMERO_CONTRAT"),F.col("RESEAU"),F.col("CANAL_DISTRIBUTION"),F.col("ITFAMPRO"),F.col("ITFAMRIS"),F.col("CDCATPR"))
.withColumn("NUMERO_CLIENT_2",lpad(F.col("NMCLI"), 10, '0'))
.drop("NMCLI")

)

Base_FCA=(df_client.join(df_contrat,["NUMERO_CLIENT_2"],how="left")


.withColumn("NUMERO_CONTRAT"  ,F.when( (F.col("CANAL_DISTRIBUTION").isin(["010","011","012","013","014","036"])) & (F.col("NUMERO_CONTRAT").substr(15,2)=="92") & (F.col("ITFAMPRO")=="EPA" ) , F.lpad(F.substring(F.col("NUMERO_CONTRAT"), 1, 14)  , 16, '0')    )
.otherwise(F.col("NUMERO_CONTRAT")))

).dropDuplicates(["NUMERO_CONTRAT"])



# COMMAND ----------

Base_FCA.write.format("parquet").mode("overwrite").saveAsTable("Base_FCA")
