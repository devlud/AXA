# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 12/04/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

pip install jours-feries-france


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import calendar
from pyspark.sql.functions import row_number 
from datetime import date
from datetime import datetime, timedelta, date
from axapy.labs.piper import NovaReader,DmtlsReader,RduReader
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
import pandas as pd

# COMMAND ----------

# MAGIC %md ### SAISIE MANUELLE

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

# COMMAND ----------

# MAGIC %md ### FCA

# COMMAND ----------

Base_FCA=spark.read.format("parquet").table("Base_FCA")

# COMMAND ----------

# MAGIC %md ### RDU

# COMMAND ----------

RDU=spark.read.format("parquet").table("RDU")

# COMMAND ----------

# MAGIC %md ### AWS

# COMMAND ----------

Base_AWS=spark.read.format("parquet").table("Base_AWS_VF")

# COMMAND ----------

# MAGIC %md ###NOVA 

# COMMAND ----------

Base_NOVA=spark.read.format("parquet").table("Base_NOVA_VF")


# COMMAND ----------

# MAGIC                                                        %md ###AGIPI

# COMMAND ----------

Base_AGIPI=spark.read.format("parquet").table("Base_AGIPI_VF")


# COMMAND ----------

# MAGIC %md ### Fichiers Externes 
# MAGIC
# MAGIC

# COMMAND ----------

file_location = "/FileStore/tables/infocentre.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

INFOCENTRE = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

INFOCENTRE=(INFOCENTRE
.withColumnRenamed("numero_contrat","numero_contrat1")
.withColumn("NUMERO_CONTRAT",lpad(F.col("numero_contrat1"), 16, '0'))       
 .drop("numero_contrat1")
 .dropDuplicates(["NUMERO_CONTRAT"])           )


# COMMAND ----------

file_location = "/FileStore/tables/CLIENTS_GP.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

Contrats_actifs_CRM_GP = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

Contrats_actifs_CRM_GP=(Contrats_actifs_CRM_GP
.withColumn("NUMERO_CLIENT_2",F.lpad(F.col("NUMERO_CLIENT"), 10, '0'))       
.dropDuplicates(["NUMERO_CLIENT_2"])
.drop("NUMERO_CLIENT"))


# COMMAND ----------

file_location = "/FileStore/tables/Portefeuilles_actifs_CRM_PF.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

Portefeuilles_actifs_CRM_PF = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

Portefeuilles_actifs_CRM_PF=(Portefeuilles_actifs_CRM_PF
.withColumn("CODE_PORTEFEUILLE",F.lpad(F.col("CODE_PORTEFEUILLE"), 10, '0'))       
.dropDuplicates(["CODE_PORTEFEUILLE"])           )

# COMMAND ----------

file_location = "/FileStore/tables/PMO.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

PMO= spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

PMO=(PMO
.withColumn("NUMERO_CONTRAT",F.lpad(F.col("NMCNT"), 16, '0'))
.drop("NMCNT")       
.dropDuplicates(["NUMERO_CONTRAT"])        )

# COMMAND ----------

file_location = "/FileStore/tables/topage_TMGA.csv"
file_type = "csv"

infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

TOPAGE_TMGA = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)\
  .drop("_c2")\
  .dropDuplicates(["CLE"])           

# COMMAND ----------

# MAGIC %md ### LA TABLE CANAL

# COMMAND ----------

df_canal = spark.createDataFrame([ 
                               
('001','GUICHET NATIO',''),
('002','PLATEFORME NATIO',''),
('003','DOM-TOM',''),
('004','DOM-TOM',''),
('005','ESPACES AXA',''),
('006','MICHELIN',''),
('007','MICHELIN',''),
('008','MICHELIN',''),
('009','PHONE ASSUR',''),
('010','AGA','RESEAU PROPRIETAIRE'),
('011','A2P','RESEAU PROPRIETAIRE'),
('012','A2P','RESEAU PROPRIETAIRE'),
('013','AEP','RESEAU PROPRIETAIRE'),
('014','AEP','RESEAU PROPRIETAIRE'),
('015','JURIDICA',''),
('016','COURTIER MSC','RESEAU TIERS'),
('017','DELEGUE MSC','RESEAU TIERS'),
('018','MONVOISIN','RESEAU TIERS'),
('019','ASSU2000','RESEAU TIERS'),
('020','UNOFI','RESEAU TIERS'),
('021','ASTRAL','RESEAU TIERS'),
('022','CII2','RESEAU TIERS'),
('023','COURTIERS GENERALISTES','RESEAU TIERS'),
('024','AXA PARTENAIRE',''),
('025','AXA PARTENAIRE',''),
('026','AXA PARTENAIRE',''),
('027','DIVERS AXA',''),
('028','COURTIERS GENERALISTES','RESEAU TIERS'),
('029','PTF DE FORMATION',''),
('030','COURTIERS GENERALISTES','RESEAU TIERS'),
('031','COURTIERS GENERALISTES','RESEAU TIERS'),
('032','COURTIERS GENERALISTES','RESEAU TIERS'),
('033','PARTENAIRE BANCAIRE','RESEAU TIERS'),
('034','PARTENAIRE DISTRIBUTION','RESEAU TIERS'),
('035','CGPI','RESEAU TIERS'),
('036','GESTION PRIVEE DIRECTE','GESTION PRIVEE'),
('037','COURTIERS GENERALISTES','RESEAU TIERS'),
('038','WEB AXA',''),
('039','AXA PJ','')


           
                              ], 
                                ("CANAL_RDU_PRINCIPAL","LIBNAME_RESEAU","LIBNAME_CANAL")
                             )


# COMMAND ----------

# MAGIC %md ###BASE EPARGNE

# COMMAND ----------


windowSpec  = Window.partitionBy("NUMERO_CONTRAT").orderBy("NUMERO_CONTRAT")


Base_Epargne_Finale=(
  
    ((Base_AGIPI.union(Base_AWS).union(Base_NOVA))
     .withColumn( "CLE" ,F.concat_ws("",F.col("CODE_PRODUIT") ,  F.col("CODE_UT")   )       ))
    .withColumnRenamed("NUMERO_CLIENT","NUMERO_CLIENT_1") 


    .join(RDU,["CODE_PORTEFEUILLE"],how="left")
    .join(Base_FCA,['NUMERO_CONTRAT'],how="left")
    .join(df_canal,['CANAL_RDU_PRINCIPAL'],how="left")
    .join(INFOCENTRE,['NUMERO_CONTRAT'],how="left")
    .join(TOPAGE_TMGA ,['CLE'],how="left")


.withColumn("CNT",F.when( row_number().over(windowSpec)=="1","1").otherwise("0")                                     )


.withColumn("TYPE_UC",F.when(F.col("CODE_ISIN")=="FR0011129717",F.lit("ASI"))   
                      .when(F.col("CODE_ISIN")=="FR0013076031",F.lit("ASIS")).otherwise(F.lit(""))       )


.withColumn("CNT",F.when( row_number().over(windowSpec)=="1","1").otherwise("0")                                     )

# Création de la variable RESEAU
.withColumn("RESEAU",
     
            F.when(F.col("LIBNAME_RESEAU").isNull(),F.col("LB_CANAL"))
            .otherwise(F.col("LIBNAME_RESEAU")))
.withColumn("RESEAU",(
     
            when(F.col("RESEAU").contains("A2P"),F.lit("A2P"))
            .when(F.col("RESEAU").isin(["AEP","AEP PRIAM","SALARIES"]),F.lit("AEP"))
            . when(F.col("RESEAU").isin(["AGENTS GENERAUX","AGA"]),F.lit("AGA"))
            . when(F.col("RESEAU").isin(["GESTION PRIVEE","Gestion Privée Directe"]),F.lit("GESTION PRIVEE DIRECTE"))
            . when(F.col("RESEAU").isin(["Partenariats Bancaires","PARTENAIRE BANCAIRE"]),F.lit("PARTENAIRE BANCAIRE"))
            . when(F.col("RESEAU").isin(["Partenariats Distribution","PARTENAIRE ASSURANCE"]),F.lit("PARTENAIRE DISTRIBUTION"))
            . when(F.col("RESEAU").isin(["Courtiers Spécialistes","CGPI"]),F.lit("CGPI"))
            . when(F.col("RESEAU").isin(["COURTIERS AXA FRANCE","Courtiers Généralistes"]),F.lit("COURTIERS GENERALISTES"))
            . when(F.col("RESEAU").isin(["AUTRE DOM","ESPACE CONSEIL DOM"]),F.lit("DOM-TOM"))
            . when(F.col("RESEAU").isin(["AGENT AXA PARTENAIRE","SALARIES AXA PARTENA"]),F.lit("AXA PARTENAIRE"))
            . when(F.col("RESEAU").isin(["PLATEFORMES PRIAM"]),F.lit("PRIAM"))).otherwise(F.col("RESEAU"))
            )

# On corrige la variable RESEAU POUR AGIPI
.withColumn("RESEAU",
     
            when( (F.col("RESEAU")=="ND") &  (F.col("SYSTEME")=="AGIPI"),F.col("RESEAU_SAS"))
            .otherwise(F.col("RESEAU")))

.withColumn("LIBELLE_COMMERCIAL",
     
            when( (F.col("LIBELLE_COMMERCIAL").isNull()) &  (F.col("SYSTEME")=="AGIPI"),F.col("LIBELLE_COMMERCIAL_SAS"))
            .otherwise(F.col("LIBELLE_COMMERCIAL")))

.withColumn("CANAL",
            when(F.col("RESEAU").isin(["A2P","AEP","AGA"]),F.lit("RESEAU PROPRIETAIRE"))
            .when(F.col("RESEAU").isin(["COURTIER MSC","DELEGUE MSC","MONVOISIN","ASSU2000","UNOFI","ASTRAL","CII2","COURTIERS GENERALISTES","PARTENAIRE BANCAIRE","PARTENAIRE DISTRIBUTION","CGPI"]),
            F.lit("RESEAU TIERS"))
            .when(F.col("RESEAU").isin(["GESTION PRIVEE DIRECTE"]),F.lit("GESTION PRIVEE"))
            .otherwise(F.lit("AUTRES")))  

# Dans les tables AWS, NOVA et 
.withColumn("NUMERO_CLIENT",
            F.when((F.col("NUMERO_CLIENT_2")!=F.col("NUMERO_CLIENT_1")) | (F.col("NUMERO_CLIENT_1").isNull()),F.col("NUMERO_CLIENT_2") )
            .otherwise(F.col("NUMERO_CLIENT_1"))

)

 # récupération de l'informatin de GP RESEAU           

.withColumn("CANAL",
            when( ((F.col("CANAL").contains("RESEAU PROPRIETAIRE")) & (F.col("SOUS_SEGPAT").isin("GP")) ) | 
                 ( (F.col("CANAL").contains("RESEAU PROPRIETAIRE")) & (F.col("SYSTEME").isin("AWS")))
            ,F.lit("GESTION PRIVEE")).otherwise(F.col("CANAL"))   )
         

.withColumn("TOP_ONE",
            when( (F.col("SYSTEME")=="AWS") | (F.col("CANAL").isin(["RESEAU TIERS","GESTION PRIVEE"]) ),1).otherwise(0))


.withColumn("TOP_TMGA", F.when( F.col("TMGA")=="O" ,   F.lit("O")).otherwise(F.lit("N")))
.withColumn("DATE_NAISSANCE",date_format("DATE_NAISSANCE","dd/MM/yyyy"))

#bloc pour supprimer les contrats sans informations qui n'ont aucun flux et ou PM sur l'année d'étude            
.withColumn("CUMUL", F.col('Collecte_Brute_TOTALE')+F.col('Prestation_TOTALE')+F.col('Arbitrage_Entrant')+F.col('Arbitrage_Sortant')+F.col('PM_DEBUT')+F.col('PM_FIN')
           )
.withColumn("CUMUL_1", F.sum(F.col("CUMUL")).over(windowSpec))
.filter(F.col('CUMUL_1')!=0)
.drop("CUMUL","CUMUL_1")



#########################################################################            La PARTIE GP           ########################################################################################################


.join(Contrats_actifs_CRM_GP.drop("NUMERO_ABONNE"),["NUMERO_CLIENT_2"],how="left")
.join(PMO,["NUMERO_CONTRAT"],how="left")
.join(Portefeuilles_actifs_CRM_PF,["CODE_PORTEFEUILLE"],how="left")




 .withColumn("LIBELLE_COMMERCIAL_CRM",F.when(   (F.col("RESEAU").isin(["A2P","AGA","A2P","GESTION PRIVEE DIRECTE"])) & (F.col("TOP_ONE")=="1")    ,F.col("LIBELLE_COMMERCIAL_GP"))
                                      .when( (F.col("RESEAU").isin(["COURTIERS GENERALISTES","CGPI","PARTENAIRE BANCAIRE","PARTENAIRE DISTRIBUTION"])) & (F.col("TOP_ONE")=="1"),F.col("LIBELLE_COMMERCIAL_PF"))
                                      .otherwise(F.col("LIBELLE_COMMERCIAL")) )
 

  .withColumn("SEGMENTATION",F.when((F.col("RESEAU").isin(["A2P","AGA","A2P","GESTION PRIVEE DIRECTE"])) & (F.col("TOP_ONE")=="1"),F.col("SEGMENTATION_GP"))
                            .when( (F.col("RESEAU").isin(["COURTIERS GENERALISTES","CGPI","PARTENAIRE BANCAIRE","PARTENAIRE DISTRIBUTION"])) & (F.col("TOP_ONE")=="1"),F.col("SEGMENTATION_PF"))
                            .otherwise(F.lit(" "))  )        
  .withColumn("APPORTEUR",F.when((F.col("RESEAU").isin(["A2P","AGA","A2P","GESTION PRIVEE DIRECTE"])) & (F.col("TOP_ONE")=="1"),F.col("APPORTEUR_GP"))
                           .when((F.col("RESEAU").isin(["COURTIERS GENERALISTES","CGPI","PARTENAIRE BANCAIRE","PARTENAIRE DISTRIBUTION"])) & (F.col("TOP_ONE")=="1"),F.col("APPORTEUR_PF"))
                           .otherwise(F.lit(" "))             
           )
 
  .withColumn("TISSIER",F.when( F.col("CODE_PORTEFEUILLE").contains("033066000"),F.lit("Oui"))
                                  .otherwise(F.lit("Non")) 
              )

 .withColumn("Arbitrage_Entrant_UC",F.when(F.col("TYPE_SUPPORT").isin(["UC"]),F.col("Arbitrage_Entrant")).otherwise(0))             
 .withColumn("Arbitrage_Entrant_EURO",F.when(F.col("TYPE_SUPPORT").isin(["EURO"]),F.col("Arbitrage_Entrant")).otherwise(0))              
 .withColumn("Arbitrage_Entrant_EUROCROISSANCE",F.when(F.col("TYPE_SUPPORT").isin(["EUROCROISSANCE"]),F.col("Arbitrage_Entrant")).otherwise(0))     

 .withColumn("Arbitrage_Sortant_UC",F.when(F.col("TYPE_SUPPORT").isin(["UC"]),F.col("Arbitrage_Sortant")).otherwise(0))             
 .withColumn("Arbitrage_Sortant_EURO",F.when(F.col("TYPE_SUPPORT").isin(["EURO"]),F.col("Arbitrage_Sortant")).otherwise(0))              
 .withColumn("Arbitrage_Sortant_EUROCROISSANCE",F.when(F.col("TYPE_SUPPORT").isin(["EUROCROISSANCE"]),F.col("Arbitrage_Sortant")).otherwise(0))
 
 .withColumn("TYPE_PRSN",
                          F.when((F.col("SYSTEME")=="NOVA") & (F.col("TYPE")=="DOUBLE PROFIL") & (F.col("CIVILITE_PM").isin(["ASSOC","CAB","SOC","SYND","SARL","ENTR","STE","CBT","ASSO"])),F.lit("MOR"))
                          .when((F.col("SYSTEME")=="NOVA")&(F.col("TYPE")=="FICHE PM"),F.lit("MOR"))
                          .when((F.col("SYSTEME")=="NOVA") & (F.col("TYPE")=="DOUBLE PROFIL") & (~ F.col("CIVILITE_PM").isin(["ASSOC","CAB","SOC","SYND","SARL","ENTR","STE","CBT","ASSO"])),F.lit("PHY")).when((F.col("SYSTEME")=="NOVA") & (F.col("TYPE")=="FICHE PP"),F.lit("PHY"))
                          .when((F.col("SYSTEME")=="NOVA") & (~ F.col("TYPE").isin(["DOUBLE PROFIL","FICHE PM","FICHE PP"])),F.lit("ND")) 
                          .otherwise(F.col("TYPE_PRSN")))
        


.withColumn("TYPE_PRSN",F.when(F.col("TYPE_PRSN").isNull(),F.lit("PHY"))
                         .otherwise(F.col("TYPE_PRSN")))
  

             
             
             
             
             )
 
Base_Epargne_Finale=(Base_Epargne_Finale

.groupBy(

F.col("NUMERO_CONTRAT"),F.col("CODE_PRODUIT"),F.col("NOM_PRODUIT"),F.col("SITUATION_CLOTURE"),F.col("DATE_EFFET"),F.col("DATE_TERME"),F.col("DT_VL_FIN"),F.col("CODE_ISIN"),F.col("NOM_SUPPORT"),F.col("SUPPORT_SPECIFIQUE"),
F.col("CODE_UT") ,F.col("TYPE_SUPPORT"),F.col("TYPE_UC"),F.col("TYPE_GESTION"),F.col("PROFIL_GESTION"),F.col("CODE_PORTEFEUILLE"),F.col("NUMERO_CLIENT"),F.col("NUMERO_ABONNE"),F.col("UJ"),F.col("TYPE_PRSN"),F.col("CIVILITE"),F.col("NOM_PRENOM_SOUSCRIPTEUR"),
F.col("DATE_NAISSANCE"),F.col("CODE_CSP"),F.col("ADRESSE"),F.col("CODE_POSTAL"),F.col("VILLE"),F.col("PAYS"),F.col("EMAIl"),F.col("LIBELLE_COMMERCIAL"),F.col("LIBELLE_COMMERCIAL_CRM"),F.col("APPORTEUR"),F.col("CODE_DELEGATION"),F.col("DELEGATION"),
F.col("CODE_REGION"),F.col("REGION"),F.col("CODE_GESTIONNAIRE"),F.col("CANAL_RDU_PRINCIPAL"),F.col("CANAL"),F.col("RESEAU"),F.col("SEGPAT"),F.col("SOUS_SEGPAT"),F.col("CIVILITE_PM"),F.col("NATURE_PMO"),F.col("TOP_ONE"),F.col("SEGMENT"),
F.col("SEGMENTATION"),F.col("TOP_TMGA"),F.col("TISSIER"),F.col("TOP_RETRAITE"),F.col("SYSTEME"),F.col("CNT"))



.agg(

#F.sum(F.col("Affaire_Nouvelle")).alias("Affaire_Nouvelle"),
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
F.sum(F.col("Arbitrage_Entrant_UC")).alias("Arbitrage_Entrant_UC"),
F.sum(F.col("Arbitrage_Entrant_EURO")).alias("Arbitrage_Entrant_EURO"),
F.sum(F.col("Arbitrage_Entrant_EUROCROISSANCE")).alias("Arbitrage_Entrant_EUROCROISSANCE"),
F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrage_Sortant"),
F.sum(F.col("Arbitrage_Sortant_RAG")).alias("Arbitrage_Sortant_RAG"),
F.sum(F.col("Arbitrage_Sortant_Client")).alias("Arbitrage_Sortant_Client"),
F.sum(F.col("Arbitrage_Sortant_UC")).alias("Arbitrage_Sortant_UC"),
F.sum(F.col("Arbitrage_Sortant_EURO")).alias("Arbitrage_Sortant_EURO"),
F.sum(F.col("Arbitrage_Sortant_EUROCROISSANCE")).alias("Arbitrage_Sortant_EUROCROISSANCE"),
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN"))

.withColumn("Affaire_Nouvelle",F.when( (F.substring(F.col("DATE_EFFET"),7,4))==annee,F.col("Collecte_Brute_TOTALE") ).otherwise(0)                                      )

.select(F.col("NUMERO_CONTRAT"),F.col("CODE_PRODUIT"),F.col("NOM_PRODUIT"),F.col("SITUATION_CLOTURE"),F.col("DATE_EFFET"),F.col("DATE_TERME"),F.col("DT_VL_FIN"),F.col("CODE_ISIN"),F.col("NOM_SUPPORT"),F.col("SUPPORT_SPECIFIQUE"),
F.col("CODE_UT") ,F.col("TYPE_SUPPORT"),F.col("TYPE_UC"),F.col("TYPE_GESTION"),F.col("PROFIL_GESTION"),F.col('Affaire_Nouvelle'),F.col("Collecte_Brute_TOTALE"),F.col("Versement_Initial"),F.col("Versement_Complementaire"),F.col("Prelevement_Automatique"),
F.col("Transfert_Entrant")	,F.col("Prestation_TOTALE")	,F.col("Rachat"),F.col("Sinistre"),F.col("Echu"),F.col("Transfert_Sortant"),F.col("Arbitrage_Entrant"),F.col("Arbitrage_Entrant_RAG")	,F.col("Arbitrage_Entrant_Client") ,
F.col("Arbitrage_Entrant_UC"),F.col("Arbitrage_Entrant_EURO"),F.col("Arbitrage_Entrant_EUROCROISSANCE"),F.col("Arbitrage_Sortant")	,F.col("Arbitrage_Sortant_RAG")	,F.col("Arbitrage_Sortant_Client"),F.col("Arbitrage_Sortant_UC"),
F.col("Arbitrage_Sortant_EURO"),F.col("Arbitrage_Sortant_EUROCROISSANCE"),F.col("PM_DEBUT"),F.col("PM_FIN"),F.col("CODE_PORTEFEUILLE"),F.col("NUMERO_CLIENT"),F.col("NUMERO_ABONNE"),F.col("UJ"),F.col("TYPE_PRSN"),F.col("CIVILITE"),
F.col("NOM_PRENOM_SOUSCRIPTEUR"),F.col("DATE_NAISSANCE"),F.col("CODE_CSP"),F.col("ADRESSE"),F.col("CODE_POSTAL"),F.col("VILLE"),F.col("PAYS"),F.col("EMAIl"),F.col("LIBELLE_COMMERCIAL"),F.col("LIBELLE_COMMERCIAL_CRM"),F.col("APPORTEUR"),
F.col("CODE_DELEGATION"),F.col("DELEGATION"),F.col("CODE_REGION"),F.col("REGION"),F.col("CODE_GESTIONNAIRE"),F.col("CANAL_RDU_PRINCIPAL"),F.col("CANAL"),F.col("RESEAU"),F.col("SEGPAT"),F.col("SOUS_SEGPAT"),F.col("CIVILITE_PM"),F.col("NATURE_PMO"),
F.col("TOP_ONE"),F.col("SEGMENT"),F.col("SEGMENTATION"),F.col("TOP_TMGA"),F.col("TISSIER"),F.col("TOP_RETRAITE"),F.col("SYSTEME"),F.col("CNT"))


.sort(["NUMERO_CONTRAT"],descending=True).na.fill(value=0) 

)

# COMMAND ----------

 #On enregistre la Base Epargne
Base_Epargne_Finale.withColumn("func_month",F.lit(annee + mois)).write.format('DELTA').partitionBy('func_month').mode('append').option("overwriteSchema", "true").save('/mnt/base_sauv/Base_Epargne_Finale.DELTA')
#Base_Epargne_Finale.withColumn("func_month",F.lit(annee + mois)).write.format('DELTA').partitionBy('func_month').mode('overwrite').option("overwriteSchema", "true").save('/mnt/base_sauv/Base_Epargne_Finale.DELTA')

# COMMAND ----------

#from delta.tables import*
#import pyspark.sql.functions as F
#deltaTable = DeltaTable.forPath(spark,'/mnt/base_sauv/Base_Epargne_Finale.DELTA' )
#deltaTable.delete(F.col('func_month') == (annee + mois))

# COMMAND ----------

# On enregistre la Base Epargne

#Base_Epargne_Finale.withColumn("func_month",F.lit(annee + mois)).write.format('DELTA').partitionBy('func_month').mode('append').save('/mnt/base_sauv/Base_Epargne_Finale.DELTA')

# COMMAND ----------

# On enregistre la Base One 

#Base_One.withColumn("func_month",F.lit(annee + mois)).write.format('DELTA').partitionBy('func_month').mode('append').save('/mnt/base_sauv/Base_One.DELTA')

# COMMAND ----------

# On sauvgarde la Base One dans le PowerBi 

#Base_One.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Base_One.Base_One")

# COMMAND ----------

#df_BAse_Epargne = spark.read.format('DELTA').load('/mnt/base_sauv/Base_Epargne_Finale.DELTA').where(func_month="202307") 

# COMMAND ----------

#%sql
#ALTER TABLE Base_One.Base_One
#RENAME TO Base_One_2307;
