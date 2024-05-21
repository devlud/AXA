# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 27/02/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import when, datediff, dayofmonth, substring, month, year, lit, desc, concat_ws, regexp_replace, dense_rank, row_number, lag, max, lead, to_date, countDistinct, current_date, date_format, concat_ws
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import calendar
from axapy.labs.piper import AwsReader
AwsRead = AwsReader()
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier


annee_N_1= str((int(annee)-1))
jour=str(calendar.monthrange(int(annee), int(mois))[1])
Periode = [""+annee+"-01-01",""+annee+"-"+mois+"-"+jour+""]


# COMMAND ----------

from pyspark.sql.functions import lpad
Table_SEGMENT = spark.read.format("csv") \
  .option("header", "true") \
  .option("sep", ";") \
  .load("dbfs:/FileStore/Power BI/cartographie_segments_revue.csv")\
.withColumn("CODE_UT", F.when((F.col("CODE_UT")*1)>0,lpad(F.col("CODE_UT"),5,'0')).otherwise(F.col("CODE_UT")))\
.withColumn("CODE_PRODUIT", F.when((F.col("CODE_UV")*1)>0,lpad(F.col("CODE_UV"),5,'0')).otherwise(F.col("CODE_UV")))\
.select(F.col("CODE_UT"),F.initcap(F.col('SEGMENT')).alias("SEGMENT"),F.col("CODE_PRODUIT"))\
.distinct()

# COMMAND ----------

liste_supports_spé= ["FR0013076031", "FR0011129717", "FR0013466562", "FR0013473543", "FR0010188334", "FR0125508632", "FR0123426035", "FR0122689674", "FR0122540299", "FR0124236763", "FR0122338959", "IE00B8501520", "FR0122919402", "FR0122919428", "FR0123169676", " LU1719067172", "IE00BDBVWQ24", "IE00BHY3RV36", "FR001400CZ43"]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE PM

# COMMAND ----------

#Récupération des PM de l'année N

#windowSpec  = Window.partitionBy("ID_CONTRAT","CD_SUPPORT","CD_FUND").orderBy(F.col("DT_VL_FIN").desc())

PM_N = (
    AwsRead.read_table("pdd_contrat_support",date_limite=""+annee+"-"+mois+"-"+jour+"")
                    .select(F.col("ID_CONTRAT"),F.col("CD_SUPPORT"),F.col("CD_FUND")
                            ,F.col("CD_PACK"),F.col("CD_OG"),F.col("PM_AXIOME").alias("PM_FIN"),F.col("NMBR_UC").alias("NB_UC_FIN")
                           ,F.col("VL_RACHAT").alias("VL_FIN"),F.col("MT_NON_INVESTI").alias("MT_NON_INVESTI")
                           ,date_format(F.col("DT_POSITION"),"dd/MM/yyyy").alias("DT_VL_FIN"))
                   .withColumn("CD_OG",F.when(F.col("CD_OG").isNull(),F.lit("libre")).otherwise(F.col("CD_OG"))     )   
                               
                    .sort(["ID_CONTRAT"],descending=True).na.fill(value=0) 


#.withColumn("TOP_LAST",dense_rank().over(windowSpec)).filter(F.col("TOP_LAST")=="1").drop("TOP_LAST")
)             
#Récupération des PM de l'année N-1                 
PM_N_1 = (
    AwsRead.read_table("pdd_contrat_support",""+ annee_N_1 +"-12-31")
                    .select(F.col("ID_CONTRAT"),F.col("CD_SUPPORT"),F.col("CD_FUND")
                            ,F.col("CD_PACK"),F.col("CD_OG"),F.col("PM_AXIOME").alias("PM_DEBUT"),F.col("NMBR_UC").alias("NB_UC_DEBUT")
                           ,F.col("VL_RACHAT").alias("VL_DEBUT"))
     .withColumn("CD_OG",F.when(F.col("CD_OG").isNull(),F.lit("libre")).otherwise(F.col("CD_OG"))     )   
    .sort(["ID_CONTRAT"],descending=True).na.fill(value=0)
)
#Création de la base des PM de l'année
Base_PM=(PM_N.join(PM_N_1,["ID_CONTRAT","CD_FUND","CD_SUPPORT","CD_PACK","CD_OG"],how="full" )

         .groupby(["ID_CONTRAT","CD_FUND","CD_SUPPORT","CD_PACK","CD_OG","DT_VL_FIN","VL_DEBUT","NB_UC_DEBUT","VL_FIN","NB_UC_FIN"])
         .agg(
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN"),
F.sum(F.col("MT_NON_INVESTI")).alias("MT_NON_INVESTI")             
         )
    .na.fill(value=0)

)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE FLUX

# COMMAND ----------

Base_Mvt_dtl=(AwsRead.read_table("pdd_mouvement_dtl_histo")
    .select(["DT_INVEST","ID_CONTRAT","CD_OG","CD_PACK","CD_SUPPORT","LB_TYPE_FLUX","CD_FUND","MT_BRUT","MT_INVESTI","ID_MVT"])
    .join( AwsRead.read_table("pdd_mouvement_histo").select(["ID_MVT","ID_CONTRAT","LB_STATUT","DT_SAISIE","DT_CNVRSN","LB_NATURE_MVT","LB_TYPE_MVT"]).distinct(), ["ID_CONTRAT","ID_MVT"],how="left") 
    
           # .filter(((year('DT_SAISIE')==annee)|(year('DT_CNVRSN')==annee)|(year('DT_INVEST')==annee))&((F.col('DT_INVEST') >= Periode[0]) & (F.col('DT_INVEST') <= Periode[1])))
            .filter((F.col("DT_INVEST")>=Periode[0])&(F.col("DT_INVEST")<=Periode[1]))
            
            .filter(F.col("LB_STATUT")=="Soldé")
            
            .withColumn("MONTANT_NET",F.col("MT_INVESTI"))
            .withColumn("MONTANT_BRUT",F.col("MT_BRUT"))
    
    #Collecte
.withColumn("Versement_Initial", F.when(F.col("LB_TYPE_FLUX").isin(["Prime comptant","Transfert externe entrant"]),F.col("MT_BRUT")).otherwise(0))
.withColumn("Versement_Complementaire", F.when(F.col("LB_TYPE_FLUX").isin(["Versement libre"]),F.col("MT_BRUT")).otherwise(0))
.withColumn("Prelevement_Automatique", F.when(F.col("LB_TYPE_FLUX").isin(["Versement libre programmé"]),F.col("MT_BRUT")).otherwise(0)) 
.withColumn("Transfert_Entrant", F.when( F.col("LB_TYPE_FLUX").isin(["Transfert interne entrant"]) ,F.col("MT_BRUT")).otherwise(0))
.withColumn("Collecte_Brute_TOTALE", F.col("Versement_Initial")+F.col("Versement_Complementaire")+F.col("Prelevement_Automatique")+F.col("Transfert_Entrant"))
       
    #Arbitrage
.withColumn("Arbitrage_Entrant", F.when((F.col("LB_TYPE_FLUX")=="Arbitrage achat"),F.col("MT_BRUT")).otherwise(0))       
.withColumn("Arbitrage_Entrant_RAG", F.when(((F.col("LB_TYPE_FLUX")=="Arbitrage achat")&(F.col("LB_TYPE_MVT").isin(["RAGALD","RAG"]))),F.col("MT_BRUT")).otherwise(0)) 
.withColumn('Arbitrage_Entrant_Client', F.col('Arbitrage_Entrant')-F.col('Arbitrage_Entrant_RAG'))

.withColumn("Arbitrage_Sortant", F.when((F.col("LB_TYPE_FLUX")=="Arbitrage vente"),F.col("MT_BRUT")).otherwise(0))       
.withColumn("Arbitrage_Sortant_RAG", F.when(((F.col("LB_TYPE_FLUX")=="Arbitrage vente")&(F.col("LB_TYPE_MVT").isin(["RAGALD","RAG"]))),F.col("MT_BRUT")).otherwise(0)) 
.withColumn('Arbitrage_Sortant_Client', F.col('Arbitrage_Sortant')-F.col('Arbitrage_Sortant_RAG'))
    
    #Prestations
.withColumn("Rachat", F.when((F.col("LB_TYPE_FLUX").isin(["Rachat partiel","Rachat total","rachat programmé","Transfert externe sortant"])),F.col("MT_BRUT")).otherwise(0)) 
.withColumn("Sinistre", F.when(((F.col("LB_TYPE_FLUX")=="Paiement dossier sinistre")&(F.col("LB_TYPE_MVT")!="Désinvestissement échu")),F.col("MT_BRUT")).otherwise(0))
.withColumn("Echu", F.when(((F.col("LB_TYPE_FLUX")=="Paiement dossier sinistre")&(F.col("LB_TYPE_MVT")=="Désinvestissement échu")),F.col("MT_BRUT")).otherwise(0))
.withColumn("Transfert_Sortant", F.when(F.col("LB_TYPE_FLUX").isin(["Transfert interne sortant"]),F.col("MT_BRUT")).otherwise(0))
.withColumn('Prestation_TOTALE', F.col('Rachat')+F.col('Echu')+F.col('Sinistre')+F.col('Transfert_Sortant'))  
    

  
   #Autres
.withColumn("Frais_Mandat", F.when(F.col("LB_TYPE_FLUX").isin(["Frais de mandat","Frais de gestion conseillée"]),F.col("MT_BRUT")).otherwise(0))
.withColumn("Frais_Acquisition", F.when(F.col("LB_TYPE_FLUX").isin(["Frais sur investissement"]),F.col("MT_BRUT")).otherwise(0))
.withColumn("Frais_Gestion", F.when(F.col("LB_TYPE_FLUX").isin(["Frais de gestion"]),F.col("MT_BRUT")).otherwise(0))            
.withColumn("Frais_Rachat", F.when(F.col("LB_TYPE_FLUX").contains('Frais Rachat'),F.col("MT_BRUT")).otherwise(0))         
           )


Base_Flux_Finale=(Base_Mvt_dtl.groupBy(["ID_CONTRAT","CD_SUPPORT","CD_FUND","CD_PACK","CD_OG"])
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

                                   
F.sum(F.col("Arbitrage_Entrant")).alias("Arbitrage_Entrant"),                                    
F.sum(F.col("Arbitrage_Entrant_RAG")).alias("Arbitrage_Entrant_RAG"),
F.sum(F.col("Arbitrage_Entrant_Client")).alias("Arbitrage_Entrant_Client"), 
F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrage_Sortant"), 
F.sum(F.col("Arbitrage_Sortant_RAG")).alias("Arbitrage_Sortant_RAG"),
F.sum(F.col("Arbitrage_Sortant_Client")).alias("Arbitrage_Sortant_Client"),                                   
                                   
F.sum(F.col("Frais_Mandat")).alias("Frais_Mandat"),                                    
F.sum(F.col("Frais_Acquisition")).alias("Frais_Acquisition"),
F.sum(F.col("Frais_Gestion")).alias("Frais_Gestion"), 
F.sum(F.col("Frais_Rachat")).alias("Frais_Rachat")
                                 ).na.fill(value=0)  
.withColumn("CD_OG",F.when(F.col("CD_OG").isNull(),F.col("CD_PACK")).otherwise(F.col("CD_OG"))     )                                         
                               )


# COMMAND ----------

window = Window.partitionBy("ID_CONTRAT")

Base_PM_Flux=(Base_Flux_Finale.join(Base_PM,["ID_CONTRAT","CD_SUPPORT","CD_FUND","CD_PACK","CD_OG"],how="full")
              .groupBy(["ID_CONTRAT","CD_SUPPORT","CD_FUND","CD_PACK","CD_OG","DT_VL_FIN","VL_DEBUT","NB_UC_DEBUT","VL_FIN","NB_UC_FIN"])
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

                                   
F.sum(F.col("Arbitrage_Entrant")).alias("Arbitrage_Entrant"),                                    
F.sum(F.col("Arbitrage_Entrant_RAG")).alias("Arbitrage_Entrant_RAG"),
F.sum(F.col("Arbitrage_Entrant_Client")).alias("Arbitrage_Entrant_Client"), 
F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrage_Sortant"), 
F.sum(F.col("Arbitrage_Sortant_RAG")).alias("Arbitrage_Sortant_RAG"),
F.sum(F.col("Arbitrage_Sortant_Client")).alias("Arbitrage_Sortant_Client"),                                   
                                   
F.sum(F.col("Frais_Mandat")).alias("Frais_Mandat"),                                    
F.sum(F.col("Frais_Acquisition")).alias("Frais_Acquisition"),
F.sum(F.col("Frais_Gestion")).alias("Frais_Gestion"), 
F.sum(F.col("Frais_Rachat")).alias("Frais_Rachat"),
F.sum(F.col("MT_NON_INVESTI")).alias("MT_NON_INVESTI"),                                   
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN")                                   
                                   
                               ).na.fill(value=0) 
                
 #bloc pour supprimer les contrats sans informations qui n'ont aucun flux et ou PM sur l'année d'étude            
.withColumn("CUMUL", F.col('Collecte_Brute_TOTALE')+F.col('Prestation_TOTALE')+F.col('Arbitrage_Entrant')+F.col('Arbitrage_Sortant')+F.col('Frais_Gestion')+F.col('Frais_Rachat')+F.col('Frais_Mandat')+F.col('Frais_Acquisition')+F.col('PM_DEBUT')+F.col('PM_FIN')
           )
.withColumn("CUMUL_1", F.sum(F.col("CUMUL")).over(window))
.filter(F.col('CUMUL_1')!=0)
.drop("CUMUL","CUMUL_1")
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE Gestions/Produits/Supports
# MAGIC

# COMMAND ----------

Base_Gestion=(AwsRead.read_table("pdd_produit_og").select(['CD_OG','LB_OG_AIA','LB_SOC_GESTION']).distinct().withColumn("CD_OG",F.when(F.col("CD_OG").isNull(),F.lit("libre")).otherwise(F.col("CD_OG"))     )   
              .join(AwsRead.read_table('pdd_contrat_pack_og',archive=True).select("CD_PACK","LB_PACK",'CD_OG').withColumn("CD_OG",F.when(F.col("CD_OG").isNull(),F.lit("libre")).otherwise(F.col("CD_OG"))     ).dropDuplicates(["CD_OG"]),['CD_OG'],how='left')                                            
              .drop("CD_PACK") ).distinct()

Base_Support= (AwsRead.read_table("pdd_support")
               .select(F.col("NOM").alias("NOM_SUPPORT"),F.col("LB_CLASSIF_AMF"),F.col("LB_TYPE"),F.col("CD_FUND"),F.col("CD_INTERNE").alias("CODE_UT"),F.col("DT_SITUATION"))
 .sort(["DT_SITUATION"],descending=True)
               .withColumn('SUPPORT',F.when(F.col('LB_TYPE')=='EUR',"EURO")
                           .when(F.col('LB_TYPE')=='UC',"UC")
                           .when(F.col('LB_TYPE')=='EUC',"EUROCROISSANCE")
                           .otherwise(F.col('LB_TYPE')))
 .withColumn('SUPPORT_SPECIFIQUE', F.when(F.col('CD_FUND').isin(liste_supports_spé),"Oui").otherwise("Non"))         
 .dropDuplicates(["CD_FUND"])
 .drop("LB_TYPE")
 .drop("DT_SITUATION")

)


Base_Flux_dtl=Base_PM_Flux.join(Base_Gestion,["CD_OG"],how="left").join(Base_Support,["CD_FUND"],how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE Contrats/Apporteurs
# MAGIC
# MAGIC

# COMMAND ----------



Base_Souscripteur=(AwsRead.read_table("pdd_contrat_personne")
.filter(F.col("LB_ROLE").isin(["Souscripteur"]))
#.orderBy(F.col("DATE_SYSTEME").desc())                     
.withColumn("ADRESSE",F.concat_ws(" ; ",F.col("ADRESSE1"),F.col("ADRESSE2"),F.col("ADRESSE3"),F.col("ADRESSE4")))  
                        
  .select(F.col("ID_CONTRAT"),
  F.col("NUM_ABONNE"),F.col("REFERENCE_CLIENT"),
  F.col("LB_CIVILITE").alias("CIVILITE"),F.concat_ws(" ",F.col("NOM"),F.col("PRENOM")).alias("NOM_PRENOM_SOUSCRIPTEUR"),
  F.col("TYPE_PRSN"),F.col("SIREN"),F.col("SIRET"),date_format(F.col("DT_NAISSANCE"),"dd/MM/yyyy").alias("DATE_NAISSANCE"),
  F.col("ADRESSE"),F.col("CD_POSTAL"),F.col("VILLE"),F.col("PAYS"),
  F.col("EMAIL"),
  F.col("CD_CSP"),F.col("LB_CSP"))
    
.dropDuplicates(["ID_CONTRAT"])
                       )

Base_Contrat=(AwsRead.read_table('pdd_contrat')
.join((AwsRead.read_table("pdd_produit").select(F.col("CD_PROD"), F.col("LB_PROD").alias("NOM_PRODUIT"))),["CD_PROD"],how="left")

.withColumn('TOP_RETRAITE', when(F.col('LB_CADRE_FISCAL').isin('per', 'madelin'), 'RETRAITE').otherwise('EPARGNE'))

#.orderBy(col("DATE_SYSTEME").desc())              
.select(F.col("ID_CONTRAT"),F.col("CD_PROD"),F.col("NOM_PRODUIT"),F.col("LB_STATUS").alias("SITUATION_CLOTURE"),
date_format("DT_SOUSCRIPTION","dd/MM/yyyy").alias("DATE_EFFET"),
date_format(F.col("DT_CLOTURE"),"dd/MM/yyyy").alias("DATE_TERME"),
lpad(F.col("CD_PTF_RDU"), 10, '0').alias('CODE_PORTEFEUILLE')
,F.col("CD_APP_SEC").alias("CODE_APPORTEUR_SECONDAIRE"),F.col("TOP_VAUPALIERE"),
F.col("CD_UJ").alias("SOCIETE_JURIDIQUE"),
F.col("TOP_ANN"),F.col("TOP_NANTI"),F.col("TOP_TRANSFERT"),F.col("TOP_AVANCE"),F.col("TOP_RETRAITE"))
).dropDuplicates(["ID_CONTRAT"]
                 

                 )


Base_Contrat_Finale=Base_Contrat.join(Base_Souscripteur,["ID_CONTRAT"],how="left").withColumn("TOP",F.lit(1)).dropDuplicates(["ID_CONTRAT"])

#récupération des informations pour les contrats sans effet de l'année et les contrats en erreur
Base_Souscripteur_archive=(AwsRead.read_table("pdd_contrat_personne",archive=True)
.filter(F.col("LB_ROLE").isin(["Souscripteur"]))
#.orderBy(F.col("DATE_SYSTEME").desc())                     
.withColumn("ADRESSE",F.concat_ws(" ; ",F.col("ADRESSE1"),F.col("ADRESSE2"),F.col("ADRESSE3"),F.col("ADRESSE4")))  
                        
  .select(F.col("ID_CONTRAT"),
  F.col("NUM_ABONNE"),F.col("REFERENCE_CLIENT"),
  F.col("LB_CIVILITE").alias("CIVILITE"),F.concat_ws(" ",F.col("NOM"),F.col("PRENOM")).alias("NOM_PRENOM_SOUSCRIPTEUR"),
  F.col("TYPE_PRSN"),F.col("SIREN"),F.col("SIRET"),date_format(F.col("DT_NAISSANCE"),"dd/MM/yyyy").alias("DATE_NAISSANCE"),
  F.col("ADRESSE"),F.col("CD_POSTAL"),F.col("VILLE"),F.col("PAYS"),
  F.col("EMAIL"),
  F.col("CD_CSP"),F.col("LB_CSP"))
    
.dropDuplicates(["ID_CONTRAT"])
                       )

Base_Contrat_archive=(AwsRead.read_table('pdd_contrat',archive=True)
.join((AwsRead.read_table("pdd_produit").select(F.col("CD_PROD"), F.col("LB_PROD").alias("NOM_PRODUIT"))),["CD_PROD"],how="left")
.withColumn('TOP_RETRAITE', when(F.col('LB_CADRE_FISCAL').isin('per', 'madelin'), 'RETRAITE').otherwise('EPARGNE'))
#.orderBy(col("DATE_SYSTEME").desc())              
.select(F.col("ID_CONTRAT"),F.col("CD_PROD"),F.col("NOM_PRODUIT"),F.col("LB_STATUS").alias("SITUATION_CLOTURE"),
date_format("DT_SOUSCRIPTION","dd/MM/yyyy").alias("DATE_EFFET"),
date_format(F.col("DT_CLOTURE"),"dd/MM/yyyy").alias("DATE_TERME"),
lpad(F.col("CD_PTF_RDU"), 10, '0').alias('CODE_PORTEFEUILLE'),F.col("CD_APP_SEC").alias("CODE_APPORTEUR_SECONDAIRE"),F.col("TOP_VAUPALIERE"),
F.col("CD_UJ").alias("SOCIETE_JURIDIQUE"),
F.col("TOP_ANN"),F.col("TOP_NANTI"),F.col("TOP_TRANSFERT"),F.col("TOP_AVANCE"),F.col("TOP_RETRAITE"))
).dropDuplicates(["ID_CONTRAT"]
)

Base_Contrat_Finale_archive=(Base_Contrat_archive
                             .join(Base_Souscripteur_archive,["ID_CONTRAT"],how="left")
                             .withColumn("TOP",F.lit(1))
                             .withColumn('SITUATION_CLOTURE', F.lit("En erreur"))
                             .dropDuplicates(["ID_CONTRAT"])
                            
                            )


 

# COMMAND ----------

window_bis = Window.partitionBy("ID_CONTRAT")

Base_Contrat_VF=(
    Base_Contrat_Finale.union(Base_Contrat_Finale_archive)
    .withColumn("CUMUL", F.sum(F.col("TOP")).over(window_bis))
    .filter(((F.col("CUMUL") > 1) & (F.col('SITUATION_CLOTURE')!="En erreur"))|(F.col("CUMUL")==1))
    )

# COMMAND ----------

Base_AWS_VF=(Base_Flux_dtl.join(Base_Contrat_VF,["ID_CONTRAT"],how="left") 

.join(AwsRead.read_table("pdd_dstrbtr",archive=True)
.select(lpad(F.col("CD_AWS"), 10, '0').alias('CODE_PORTEFEUILLE'),F.col("CD_RESEAU"),F.col("LB_RESEAU").alias("LB_CANAL"))
.distinct(),["CODE_PORTEFEUILLE"],how="left")       

.withColumn("CODE_UT", F.when((F.col("CODE_UT")*1)>0,lpad(F.col("CODE_UT"),5,'0')).otherwise(F.col("CODE_UT")))
.withColumn("CODE_PRODUIT", F.when((F.col("CD_PROD")*1)>0,lpad(F.col("CD_PROD"),5,'0')).otherwise(F.col("CD_PROD")))

 .withColumnRenamed("ID_CONTRAT", "NUMERO_CONTRAT")
 .withColumnRenamed("CD_FUND","CODE_ISIN")
 .withColumnRenamed("LB_OG_AIA","PROFIL_GESTION")
 .withColumnRenamed("LB_SOC_GESTION","SOCIETE_GESTION")
 .withColumnRenamed("LB_CLASSIF_AMF", "CLASSE_ACTIF")
 .withColumnRenamed("CD_POSTAL","CODE_POSTAL")
 .withColumnRenamed("CD_CSP","CODE_CSP")
 .withColumnRenamed("LB_CSP","LIBELLE_CSP")
 .withColumnRenamed("SUPPORT","TYPE_SUPPORT")
  .withColumnRenamed("SOCIETE_JURIDIQUE","UJ")


           
 .join(Table_SEGMENT,["CODE_PRODUIT","CODE_UT"],how="left")
           
.na.fill("Gestion Libre",["PROFIL_GESTION"])
.na.fill("",["SOCIETE_GESTION","DATE_TERME","EMAIL","CODE_CSP","LIBELLE_CSP","REFERENCE_CLIENT","SIRET","NUM_ABONNE"])


.withColumn("MANDAT_DE_GESTION",F.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="libre")) , F.lit("Gestion Libre") )  
.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="ucgcp")) , F.lit("Gestion Conseillée au Passif") )         
.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="ucgca")) , F.lit("Gestion Conseillée à l'Actif") ) 
.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="ucded")) , F.lit("GSM Personnalisée") ) 
.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="uc")) , F.lit("GSM Collective") ) 
.when(  ((F.col("LB_PACK").isNull() ) & (F.col("CD_PACK")=="ucgph")) , F.lit("Gestion Par Horizon Retraite") ) 
.otherwise(F.col("LB_PACK")) )

 .withColumn("SYSTEME", F.lit("AWS"))           

 .withColumn("NUMERO_CLIENT", lpad(F.col("REFERENCE_CLIENT"), 10, '0'))      
.withColumn("SEGMENT",F.when(F.col("SEGMENT").isNull(),F.lit("Inconnu")).otherwise(F.col("SEGMENT")))
             
.select([lpad(F.col("NUMERO_CONTRAT"), 16, '0').alias('NUMERO_CONTRAT'),"CODE_PRODUIT","NOM_PRODUIT" ,"SITUATION_CLOTURE","DATE_EFFET","DATE_TERME","DT_VL_FIN","CODE_ISIN","NOM_SUPPORT","SUPPORT_SPECIFIQUE","CODE_UT","TYPE_SUPPORT","CLASSE_ACTIF","MANDAT_DE_GESTION","PROFIL_GESTION","SOCIETE_GESTION","Collecte_Brute_TOTALE","Versement_Initial","Versement_Complementaire","Prelevement_Automatique","Transfert_Entrant","Prestation_TOTALE","Rachat","Sinistre","Echu","Transfert_Sortant","Arbitrage_Entrant","Arbitrage_Entrant_RAG","Arbitrage_Entrant_Client","Arbitrage_Sortant","Arbitrage_Sortant_RAG","Arbitrage_Sortant_Client","Frais_Mandat","Frais_Acquisition","Frais_Gestion","Frais_Rachat","PM_DEBUT","VL_DEBUT","NB_UC_DEBUT","PM_FIN","VL_FIN","NB_UC_FIN","MT_NON_INVESTI","UJ","CODE_PORTEFEUILLE","CODE_APPORTEUR_SECONDAIRE","TOP_VAUPALIERE","CIVILITE","NOM_PRENOM_SOUSCRIPTEUR","DATE_NAISSANCE","TYPE_PRSN","NUM_ABONNE","NUMERO_CLIENT","SIRET","ADRESSE","CODE_POSTAL","VILLE","PAYS","EMAIL","CODE_CSP","LIBELLE_CSP","TOP_NANTI","TOP_TRANSFERT","TOP_AVANCE","CD_RESEAU","LB_CANAL","SEGMENT","TOP_RETRAITE","SYSTEME"])
.sort(["NUMERO_CONTRAT"],descending=True) 

)

# COMMAND ----------

Base_AWS =( Base_AWS_VF
.select(F.col("NUMERO_CONTRAT"),F.col("CODE_PRODUIT"),F.col("NOM_PRODUIT"),F.col("SITUATION_CLOTURE"),F.col("DATE_EFFET"),F.col("DATE_TERME"),F.col("DT_VL_FIN"),F.col("CODE_ISIN"),F.col("NOM_SUPPORT"),F.col("SUPPORT_SPECIFIQUE"),F.col("CODE_UT"),
F.col("TYPE_SUPPORT"),F.col("MANDAT_DE_GESTION").alias("TYPE_GESTION"),F.col("PROFIL_GESTION"),F.col("Collecte_Brute_TOTALE"),F.col("Versement_Initial")	,F.col("Versement_Complementaire")	,F.col("Prelevement_Automatique")	,F.col("Transfert_Entrant")	,F.col("Prestation_TOTALE")	,F.col("Rachat"),F.col("Sinistre")	,F.col("Echu"),F.col("Transfert_Sortant")	,F.col("Arbitrage_Entrant")	,F.col("Arbitrage_Entrant_RAG")	,F.col("Arbitrage_Entrant_Client")	,F.col("Arbitrage_Sortant")	,F.col("Arbitrage_Sortant_RAG")	,F.col("Arbitrage_Sortant_Client"),F.col("PM_DEBUT"),F.col("PM_FIN"),F.col("CODE_PORTEFEUILLE"),F.col("UJ"),F.col("NUMERO_CLIENT"),
F.col("TYPE_PRSN"),F.col("SYSTEME"),F.col("SEGMENT"),F.col("TOP_RETRAITE"),F.col("LB_CANAL")))

# COMMAND ----------

Base_AWS.write.format("parquet").mode("overwrite").saveAsTable("Base_AWS_VF")
