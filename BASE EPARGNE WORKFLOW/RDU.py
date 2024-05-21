# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 30/01/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *
import calendar
from axapy.labs.piper import RduReader
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
RduRead = RduReader()  

annee_N_1= str((int(annee)-1))
jour=str(calendar.monthrange(int(annee), int(mois))[1])

RDU=(RduRead.read_table('ptf')

.withColumn("KAGFGPTF_LIB_LONG_REG_PP_1", F.when(    ( F.col("KAGFGPTF_LIB_CANAL").contains("A2P") ) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15299","15800","15700","15400","46145","46176","46144","15600","15599"])   )    ,     F.lit("ILE DE FRANCE") )
                                           .when(    (F.col("KAGFGPTF_LIB_CANAL").contains("A2P")) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15099","15300","46361"])   )    ,     F.lit("OUEST") )
                                           .when(    (F.col("KAGFGPTF_LIB_CANAL").contains("A2P")) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15399","15500","46141"])   )    ,     F.lit("NORD-EST") )
                                           .when(    (F.col("KAGFGPTF_LIB_CANAL").contains("A2P")) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15200","15900","15499","46252","46255"])   )    ,     F.lit("SUD-EST") )
                                           .when(    (F.col("KAGFGPTF_LIB_CANAL").contains("A2P")) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15100","15199"])   )    ,     F.lit("SUD-OUEST") )
                                           .when(    (F.col("KAGFGPTF_LIB_CANAL").contains("A2P")) & (F.col("KAGFGPTF_CD_INSPE_PP_1").isin(["15699","15000"])   )    ,     F.lit("ND") )
                                           .otherwise(F.col("KAGFGPTF_LIB_LONG_REG_PP_1"))          )

     
.withColumn("KAGFGPTF_LIB_CANAL",F.when( F.col("KAGFGPTF_ID").contains("120031284"), F.lit("PARTENAIRE BANCAIRE")         )
                                 . otherwise(F.col("KAGFGPTF_LIB_CANAL")))


.withColumn("KAGFGPTF_CANAL",F.when( F.col("KAGFGPTF_ID").contains("120031284"), F.lit("033")         )
                                 . otherwise(F.col("KAGFGPTF_CANAL")))


.withColumn("KAGFGPTF_INTITULE",F.when( F.col("KAGFGPTF_INTITULE").isNull(), F.col("KAGFGPTF_LIB_NAT_GESTION_1")         )
                                 . otherwise(F.col("KAGFGPTF_INTITULE")))
                               

.select(F.col("KAGFGPTF_ID").alias("CODE_PORTEFEUILLE"),F.col("KAGFGPTF_CD_AGENCE").alias("CODE_AGENCE"),F.col("KAGFGPTF_INTITULE").alias("LIBELLE_COMMERCIAL"),F.col("KAGFGPTF_CD_CIRCO_PP_1").alias("CODE_CIRCONSCRIPTION"),
F.col("KAGFGPTF_CD_INSPE_PP_1").alias("CODE_DELEGATION"),F.col("KAGFGPTF_LIB_LONG_INSP_PP_1").alias("DELEGATION"), F.col("KAGFGPTF_CD_REGION_PP_1").alias("CODE_REGION") ,F.col("KAGFGPTF_LIB_LONG_REG_PP_1").alias("REGION"), F.col("KAGFGPTF_ID_DISTR_1").alias("CODE_GESTIONNAIRE") ,
F.col("KAGFGPTF_CANAL").alias("CANAL_RDU_PRINCIPAL"),F.col("KAGFGPTF_CD_RESEAU").alias("CD_RESEAU"),F.col("KAGFGPTF_CD_REGROUP").alias("CD_REGROUPEMENT"),F.col("KAGFGPTF_LIB_CANAL").alias("LIB_CANAL")     ).dropDuplicates(["CODE_PORTEFEUILLE"])  
.withColumn("CODE_PORTEFEUILLE",lpad(F.col("CODE_PORTEFEUILLE"), 10, '0'))

)


# COMMAND ----------

RDU.write.format("parquet").mode("overwrite").saveAsTable("RDU")
