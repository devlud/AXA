# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 10/02/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

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
NovaRead = NovaReader()
DmtlsRead = DmtlsReader()
RduRead = RduReader()

annee_N_1= str((int(annee)-1))
jour=str(calendar.monthrange(int(annee), int(mois))[1])

import datetime

def dernier_jour_sans_weekend(an_N_1):
    date = datetime.date(an_N_1, 12, 31)  # Dernière journée de l'année
    while date.weekday() >= 5:  # 5 correspond à samedi et 6 à dimanche
        date -= datetime.timedelta(days=1)  # Décrémenter d'un jour jusqu'à trouver un jour de semaine
    return date.day, date.month, date.year

an_N_1 = int(annee_N_1)  # Année souhaitée
jour_N_1, mois_N_1, an_N_1 = dernier_jour_sans_weekend(an_N_1)
Periode_N_1 = "" + str(an_N_1) + "-" + str(mois_N_1) + "-" + str(jour_N_1) + ""



Periode = [Periode_N_1,""+annee+"-01-01",""+annee+"-"+mois+"-"+jour+""]

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

window = Window.partitionBy("CODE_ACTE")
#informations sur le détail des flux afin d'avoir le même nom de flux pour tous les mêmes types de flux
Detail_flux = NovaRead.read_table('TAVCA02')\
.select(F.col("HDL_ACT_ID").alias("CODE_ACTE"),F.col("HDL_ACT_FULL_NAME").alias("LIBELLE_ACTE_LONG"),F.col("HDL_ACT_ABS_NAME").alias("CODE_ACTE_COURT"))\
.withColumn("_N_", F.count(F.col("CODE_ACTE")).over(window))\
.distinct()\
.sort(F.col("CODE_ACTE").asc(), F.col("CODE_ACTE_COURT").desc())\
.dropDuplicates(["CODE_ACTE"])\
.drop("_N_")\
.drop("CODE_ACTE_COURT")

# COMMAND ----------

#informations sur les profils de gestion en cours
table_profil_gestion = NovaRead.read_table('TAVPF01')\
.filter(F.col("EFF_D_OFF").isNull())\
.filter(F.col("PROF_SHORT_NAME")!="SU")\
    .withColumnRenamed("PROF_ID","CODE_PROFIL")\
    .withColumnRenamed("PROF_FULL_NAME","LIBELLE_PROFIL_GESTION")\
    .withColumnRenamed("CVN_GES","CONVENTION_GESTION")\
                       .withColumn('LIBELLE_CONVENTION_GESTION', 
                                                F.when(F.col("CONVENTION_GESTION")=="1","Gestion personnelle")\
                                                  .when(F.col("CONVENTION_GESTION")=="2","Gestion évolutive")\
                                                  .when(F.col("CONVENTION_GESTION")=="3","Gestion profilée")\
                                                  .when(F.col("CONVENTION_GESTION")=="4","Gestion personnalisable")\
                                                  .when(F.col("CONVENTION_GESTION")=="5","Convention de gestion")\
                                                  .when(F.col("CONVENTION_GESTION")=="6","Gestion sous Mandat")\
                                  .otherwise("Non défini"))\
    .withColumnRenamed("PROF_SHORT_NAME","LBC_TY_PFL")\
.dropDuplicates(["CODE_PROFIL","CONVENTION_GESTION"])\
.select(['CODE_PROFIL',initcap(F.col('LIBELLE_PROFIL_GESTION')).alias("LIBELLE_PROFIL_GESTION"),'CONVENTION_GESTION','LIBELLE_CONVENTION_GESTION'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE NOVA
# MAGIC

# COMMAND ----------

table_FLUX_ACTE = DmtlsRead.read_table('nv_flux_acte_epa',archive=True)
table_CONTRAT = DmtlsRead.read_table('nv_contrat_epargne',archive=True)
table_FLUX_SUPPORT = DmtlsRead.read_table('nv_flux_support',archive=True)
table_PERSONNE = DmtlsRead.read_table('nv_personne',archive=True)
table_PM_début = DmtlsRead.read_table('calcul_pm_at_date',date_limite=Periode[0],archive=False) 
table_PM_fin = DmtlsRead.read_table('calcul_pm_at_date',date_limite=Periode[2],archive=False) 
#table_PM_fin = DmtlsRead.read_table('calcul_pm_at_date',date_limite="2023-12-30")
#table_PM_début = DmtlsRead.read_table('calcul_pm_at_date',date_limite=""+annee_N_1+"-12-31",archive=False) 

personne_N0= NovaRead.read_table('tavpn01').select(lpad(F.col("PTF_ID"), 10, '0').alias("CODE_PORTEFEUILLE"),F.col("STREET").alias("ADRESSE"),F.col("CITY").alias("VILLE"),F.col("COUNTRY").alias("PAYS"),F.concat_ws(" ",F.col("MAIL_ADRESS1"),F.col("MAIL_ADRESS2")).alias("EMAIL"),lpad(F.col("PSN_ID"), 10, '0').alias("NUMERO_CLIENT") ).distinct()

liste_supports_spé_NOVA = ["FR0013076031", "FR0011129717", "FR0013466562", "FR0013473543", "FR0010188334", "FR0125508632", "FR0123426035", "FR0122689674", 
                           "FR0122540299", "FR0124236763", "FR0122338959", "IE00B8501520", "FR0122919402", "FR0122919428",  "FR0123169676", " LU1719067172", 
                           "IE00BDBVWQ24", "IE00BHY3RV36", "FR001400CZ43","FR001400G529","FR0000288946","LU2080768091","IE00B8QW1J18","FR001400IO08","FR1459AB1751","FR2CIBFS6232"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contrats NOVA

# COMMAND ----------

#création d'une table globale pour l'ensemble de informations sur le contrat du client
table_CONTRAT_1=(table_CONTRAT.orderBy(col("DATE_SYSTEME").desc())                   
    .withColumn('CODE_PORTEFEUILLE',F.when(F.length(table_CONTRAT['IDENTIFIANT_PORTEFEUILLE']) < 10, F.lpad(table_CONTRAT['IDENTIFIANT_PORTEFEUILLE'], 10, '0')).otherwise(table_CONTRAT['IDENTIFIANT_PORTEFEUILLE']))
     .withColumn('CODE_APPORTEUR_1',F.when(F.length(table_CONTRAT['CODE_APPORTEUR_1']) < 10, F.lpad(table_CONTRAT['CODE_APPORTEUR_1'], 10, '0')).otherwise(table_CONTRAT['CODE_APPORTEUR_1']))
     .withColumn('CODE_APPORTEUR_2',F.when(F.length(table_CONTRAT['CODE_APPORTEUR_2']) < 10, F.lpad(table_CONTRAT['CODE_APPORTEUR_2'], 10, '0')).otherwise(table_CONTRAT['CODE_APPORTEUR_2']))

    .withColumn('TOP_RETRAITE', when(F.col('FISCALITE').isin('MAD', 'PER', 'PRP', 'A82'), 'RETRAITE').otherwise('EPARGNE'))
    .withColumn("LB_CANAL",F.when(F.col("CANAL_PORTEFEUILLE").isNull(),F.col("RESEAU")).otherwise(F.col("CANAL_PORTEFEUILLE"))   )

    .withColumn('CODE_PRODUIT', split(table_CONTRAT['NOM_PRODUIT'], " - ").getItem(0))
    .withColumn('LIBELLE_PRODUIT', split(table_CONTRAT['NOM_PRODUIT'], " - ").getItem(1))
    .withColumn('AFFAIRE_NOUVELLE',F.when(year(table_CONTRAT['DATE_DEBUT_EFFET_CONTRAT']) == year(F.current_date()),F.lit('1')).otherwise(F.lit('0')))
    .withColumn('SYSTEME',F.lit('NOVA'))
    .withColumn('UJ',F.when(F.col('SOCIETE_ASSURANCE').contains("MUTUELLE"),F.lit("MU") ).otherwise(F.lit('SA')))         
    .select("NUMERO_CONTRAT","LIBELLE_ETAT_CONTRAT","LIBELLE_PRODUIT","CODE_PRODUIT","CODE_UV",
     date_format(F.col("DATE_DEBUT_EFFET_CONTRAT"),"dd/MM/yyyy").alias("DATE_DEBUT_EFFET_CONTRAT"), date_format(F.col("DATE_FIN_EFFET_CONTRAT"),"dd/MM/yyyy").alias("DATE_FIN_EFFET_CONTRAT"),"FRACTIONNEMENT",
     "CODE_PORTEFEUILLE","CODE_APPORTEUR_1","CODE_APPORTEUR_2","RESEAU","CANAL_PORTEFEUILLE",
     "UJ","SYSTEME","LB_CANAL",F.col("RESEAU").alias("RESEAU_NOVA"),"TOP_PREV","AFFAIRE_NOUVELLE","TOP_RETRAITE")
    ).dropDuplicates(["NUMERO_CONTRAT"])
                     


table_PERSONNE_finale=  (table_PERSONNE.orderBy(col("DATE_SYSTEME").desc()) 
.filter(F.col("ROLE_PERSONNE").isin(["Souscripteur","Assuré","modalité à traduire","Autre"]))

 
        .withColumn('NOM_PRENOM_SOUSCRIPTEUR',F.when(F.col("NOM").isNull(),F.col("QUALITE")) .otherwise(F.concat_ws(" ",F.col("NOM"),F.col("PRENOM"))))

        .withColumn('TYPE_PERSONNE',F.when(F.col("TYPE_PERSONNE")=="modalité à traduire","Personne Morale").otherwise(F.col("TYPE_PERSONNE")))

        .withColumn("TYPE_PRSN",F.when(F.col("TYPE_PERSONNE")=="Personne Physique",F.lit("PHY"))
            .when(F.col("TYPE_PERSONNE")=="Personne Morale",F.lit("MOR"))
            .otherwise(F.lit("ND"))
                )
      
        .select("NUMERO_CONTRAT","TYPE_PRSN",date_format(F.col("DATE_NAISSANCE"),"dd/MM/yyyy").alias("DATE_NAISSANCE"),"NOM_PRENOM_SOUSCRIPTEUR","PROFESSION","CODE_CSP",
        lpad(F.col("IDENTIFIANT_PERSONNE"), 10, '0').alias("NUMERO_CLIENT"),"CODE_POSTAL","QUALITE")
           ).dropDuplicates(["NUMERO_CONTRAT"])
                      
table_CONTRAT_finale=(table_CONTRAT_1.join(table_PERSONNE_finale,["NUMERO_CONTRAT"],how="left")).dropDuplicates(["NUMERO_CONTRAT"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flux NOVA

# COMMAND ----------

table_FLUX_ACTE_finale=(table_FLUX_ACTE.select("NUMERO_CONTRAT","NUMERO_ACTE","SENS_ACTE","CODE_ACTE","CODE_ACTE_COURT",'LIBELLE_MOTIF_ACTE','LIBELLE_ACTE_COMPLEMENT','MONTANT_CLIENT')
.join(Detail_flux,['CODE_ACTE'],how='left')
.drop("CODE_ACTE"))

TOP_TRANSFERT=(table_CONTRAT.select(F.col("NUMERO_CONTRAT"),F.col("MOTIF_ENTREE"),F.col("MOTIF_SORTIE"))
.withColumn("TOP_TE",when(F.col("MOTIF_ENTREE").isin(["A","F","S"]),1).otherwise(0))
.withColumn("TOP_TS",when(F.col("MOTIF_SORTIE").isin(["A","F","S"]),1).otherwise(0))
.drop("MOTIF_ENTREE")).distinct()



# COMMAND ----------

window = Window.partitionBy("NUMERO_CONTRAT", "NUMERO_ACTE", "CODE_ACTE_COURT")

table_FLUX_SUPPORT_finale= (table_FLUX_SUPPORT   
.select("NUMERO_CONTRAT","NUMERO_ACTE","SENS_ACTE","CODE_ACTE_COURT",'CODE_ISIN','CODE_UT','CODE_UC','DATE_CREATION_ACTE',"DATE_DEBUT_EFFET_ACTE",'LIBELLE_SUPPORT','NB_PART_SUPPORT_MVT','VL_SUPPORT','LIBELLE_TYPE_SUPPORT','TOP_EUROCROISSANCE','CODE_PROFIL','SUPPORT_CONVENTION',"DATE_VALORISATION")

.filter((F.col('DATE_CREATION_ACTE') >= Periode[1]) & (F.col('DATE_CREATION_ACTE') <= Periode[2])) 
.withColumn('CODE_PROFIL',F.when(col("CODE_PROFIL")=="", "01").otherwise(col("CODE_PROFIL"))) 
.join(table_profil_gestion,['CODE_PROFIL'],how='left')             
                            #jointure avec la table Flux_Acte                         
.join(table_FLUX_ACTE_finale,['NUMERO_CONTRAT','NUMERO_ACTE','SENS_ACTE','CODE_ACTE_COURT'],how='left')    
                              
             .withColumn('MONTANT_NET', round(F.col("NB_PART_SUPPORT_MVT")*F.col("VL_SUPPORT"),2))
             .withColumn("CUMUL", F.sum(F.col("MONTANT_NET")).over(window))
             .withColumn("MONTANT_BRUT", (F.col("MONTANT_NET")*F.col("MONTANT_CLIENT"))/F.col("CUMUL"))
                            
.na.fill(value=0,subset=["MONTANT_BRUT","CUMUL",'MONTANT_NET']) 
                            
.filter(table_FLUX_SUPPORT.SENS_ACTE=="Création")


 .withColumn('SUPPORT_SPECIFIQUE', 
                                  when((F.col('CODE_ISIN').isin(liste_supports_spé_NOVA)), "Oui")
                                  .otherwise("Non"))
.withColumn('SUPPORT_SPECIFIQUE', 
                                  when((F.col('SUPPORT_CONVENTION')=='Oui'), "Non")
                                  .otherwise(F.col('SUPPORT_SPECIFIQUE')))
.withColumn('TYPE_SUPPORT',
            F.when(table_FLUX_SUPPORT['TOP_EUROCROISSANCE']=="OUI",F.lit('EUROCROISSANCE'))
            .when(table_FLUX_SUPPORT["LIBELLE_TYPE_SUPPORT"]=="FOND EUROS",F.lit('EURO'))
            .otherwise(table_FLUX_SUPPORT['LIBELLE_TYPE_SUPPORT'])
           )
.withColumn('MOIS', month(F.col("DATE_VALORISATION")) )
.withColumn('ANNEE', year(F.col("DATE_VALORISATION")) )
                            
.select('NUMERO_CONTRAT', 'CODE_ACTE_COURT','LIBELLE_CONVENTION_GESTION','LIBELLE_ACTE_LONG','LIBELLE_MOTIF_ACTE','LIBELLE_ACTE_COMPLEMENT', 'CODE_ISIN','CODE_UT','CODE_UC','LIBELLE_SUPPORT', 'TYPE_SUPPORT', 'CONVENTION_GESTION', 'CODE_PROFIL','LIBELLE_PROFIL_GESTION','SUPPORT_CONVENTION', 'SUPPORT_SPECIFIQUE', 'DATE_VALORISATION', 'MOIS', 'ANNEE', 'MONTANT_NET', 'MONTANT_BRUT')

.join(TOP_TRANSFERT,["NUMERO_CONTRAT"],how="left")
                     #Collecte
 .withColumn('Versement_Initial', F.when( (F.col('CODE_ACTE_COURT').isin(["VI","CC"]) ) & (F.col("TOP_TE")==0), col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["RN","SE"]) , col("MONTANT_BRUT")*-1).otherwise(0))
 .withColumn('Versement_Complementaire', F.when( F.col('CODE_ACTE_COURT').isin(["VC"]) , col("MONTANT_BRUT")).otherwise(0) )                                
 .withColumn('Prelevement_Automatique', F.when( F.col('CODE_ACTE_COURT').isin(["PA","QT","QH"]) , col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AE","QR"]) , col("MONTANT_BRUT")*-1).otherwise(0) )
 .withColumn('Transfert_entrant', F.when( (( F.col('CODE_ACTE_COURT').isin(["VI","CC"])) & (F.col("TOP_TE")==1)   ) |(F.col('CODE_ACTE_COURT').isin(["TA","TE","VP","VT"])) , col("MONTANT_BRUT")).otherwise(0) )  
                            .withColumn('Collecte_Brute_TOTALE', F.col('Prelevement_Automatique')+F.col('Versement_Initial')+F.col('Versement_Complementaire')+F.col('Transfert_entrant'))                                                      
                        
                    #Arbitrage
.withColumn('Arbitrage_Entrant_RAG', F.when( (F.col('CODE_ACTE_COURT')=="EA")&((F.col( "LIBELLE_MOTIF_ACTE" ) == 'RAG')|(F.col("LIBELLE_ACTE_COMPLEMENT") == 'RAG')) , col("MONTANT_BRUT") ).otherwise(0))
.withColumn('Arbitrage_Sortant_RAG', F.when( (F.col('CODE_ACTE_COURT')=="SA")&((F.col( "LIBELLE_MOTIF_ACTE" ) == 'RAG')|(F.col("LIBELLE_ACTE_COMPLEMENT") == 'RAG')) , col("MONTANT_BRUT") ).otherwise(0))
.withColumn('Arbitrage_Entrant_Client', F.when( (F.col('CODE_ACTE_COURT')=="EA")&((F.col( "LIBELLE_MOTIF_ACTE" ) != 'RAG')|(F.col("LIBELLE_ACTE_COMPLEMENT") != 'RAG'))  , col("MONTANT_BRUT") ).otherwise(0))
.withColumn('Arbitrage_Sortant_Client', F.when( (F.col('CODE_ACTE_COURT')=="SA")&((F.col( "LIBELLE_MOTIF_ACTE" ) != 'RAG')|(F.col("LIBELLE_ACTE_COMPLEMENT") != 'RAG')) , col("MONTANT_BRUT") ).otherwise(0))
.withColumn('Arbitrage_Entrant', F.col('Arbitrage_Entrant_RAG')+F.col('Arbitrage_Entrant_Client'))
.withColumn('Arbitrage_Sortant', F.col('Arbitrage_Sortant_RAG')+F.col('Arbitrage_Sortant_Client'))
.withColumn('Arbitrage_Net', F.col('Arbitrage_Entrant')-F.col('Arbitrage_Sortant'))
 
   
                     #Prestations
 .withColumn('Rachat', F.when(  ( (  F.col('CODE_ACTE_COURT').isin(["RT","RP","RG"]) ) & (  F.col("TOP_TS")==0)  ), col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AP","AT","AX"]) , col("MONTANT_BRUT")*-1).otherwise(0))
 .withColumn('Echu', F.when( F.col('CODE_ACTE_COURT').isin(["EX","RE"]) , col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AH"]) , col("MONTANT_BRUT")*-1).otherwise(0))
 .withColumn('Sinistre', F.when( F.col('CODE_ACTE_COURT').isin(["SI"]) , col("MONTANT_BRUT")).otherwise(0))
 .withColumn('Transfert_Sortant',F.when(  (  F.col('CODE_ACTE_COURT').isin(["RT","RP","RG"]) ) & (  F.col("TOP_TS")==1) |( F.col('CODE_ACTE_COURT').isin(["TP","TT"])) , col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AV","AQ"]) , col("MONTANT_BRUT")*-1).otherwise(0))  
 .withColumn('Prestation_TOTALE', F.col('Rachat')+F.col('Echu')+F.col('Sinistre')+F.col('Transfert_Sortant'))                                                
    
                   #Autres
.withColumn('Frais_de_gestion', F.when( (F.col('CODE_ACTE_COURT')=="FR") , col("MONTANT_BRUT") ).otherwise(0))
.withColumn('Participation_aux_benefices', F.when( (F.col('CODE_ACTE_COURT')=="PB") , col("MONTANT_BRUT") ).otherwise(0))
           )


   #demander à Geoffroy

 #.withColumn('Rachat', F.when(  ( (            ) & (  F.col("TOP_TS")==0)  ), col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AP","AT","AX"]) , col("MONTANT_BRUT")*-1).otherwise(0))
 # .withColumn('Echu', F.when( F.col('CODE_ACTE_COURT').isin(["EX","RE"]) , col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AH"]) , col("MONTANT_BRUT")*-1).otherwise(0))
 #.withColumn('Sinistre', F.when( F.col('CODE_ACTE_COURT').isin(["SI"]) , col("MONTANT_BRUT")).otherwise(0))
 #.withColumn('Transfert_Sortant',F.when( ((F.col('CODE_ACTE_COURT').isin(["RT","RP","RG"])) & ( F.col("TOP_TS")==1)) |( F.col('CODE_ACTE_COURT').isin(["TP","TT"])) , col("MONTANT_BRUT")).when( F.col('CODE_ACTE_COURT').isin(["AV","AQ"]) , col("MONTANT_BRUT")*-1).otherwise(0))  
 #.withColumn('Prestation_TOTALE', F.col('Rachat')+F.col('Echu')+F.col('Sinistre')+F.col('Transfert_Sortant'))         


# COMMAND ----------

Base_Flux_Finale=(table_FLUX_SUPPORT_finale.groupBy(["NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT','LIBELLE_CONVENTION_GESTION','CODE_PROFIL','LIBELLE_PROFIL_GESTION','SUPPORT_SPECIFIQUE'])
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

F.sum(F.col("Frais_de_gestion")).alias("Frais_de_gestion"),
F.sum(F.col("Participation_aux_benefices")).alias("Participation_aux_benefices")
                                   
                                 ).na.fill(value=0)
                               )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Provisions Mathématiques NOVA

# COMMAND ----------



table_PM_1=(table_PM_fin
            .drop("LIBELLE_PROFIL_GESTION")
            .withColumn('CODE_PROFIL',F.when(col("CODE_PROFIL")=="", "01").otherwise(col("CODE_PROFIL"))) 
            .join(table_profil_gestion,['CODE_PROFIL'],how='left')
            
.withColumn('SUPPORT_SPECIFIQUE', F.when((table_PM_fin['SUPPORT_CONVENTION']=='Non'), "Oui")
     .otherwise("Non"))

.withColumn('TYPE_SUPPORT', F.when(table_PM_fin['TOP_EUROCROISSANCE']=="OUI",F.lit('EUROCROISSANCE'))
            .when(F.col("LIBELLE_TYPE_SUPPORT")=="FOND EUROS",F.lit('EURO'))
            .otherwise(F.col("LIBELLE_TYPE_SUPPORT"))
           )
.select("NUMERO_CONTRAT","PM_SUPPORT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT',"LIBELLE_CONVENTION_GESTION",'LIBELLE_PROFIL_GESTION','CODE_PROFIL','SUPPORT_SPECIFIQUE',"DATE_VALEUR_UC","VALEUR_UC","LIBELLE_SUPPORT")
            
           .withColumnRenamed("PM_SUPPORT","PM_FIN")
           )
            
            
table_PM_2=(table_PM_début
            .drop("LIBELLE_PROFIL_GESTION")
            .withColumn('CODE_PROFIL',F.when(col("CODE_PROFIL")=="", "01").otherwise(col("CODE_PROFIL"))) 
            .join(table_profil_gestion,['CODE_PROFIL'],how='left')
            
.withColumn('SUPPORT_SPECIFIQUE', F.when((table_PM_début['SUPPORT_CONVENTION']=='Non'), "Oui")
     .otherwise("Non"))

.withColumn('TYPE_SUPPORT', F.when(table_PM_début['TOP_EUROCROISSANCE']=="OUI",F.lit('EUROCROISSANCE'))
            .when(table_PM_début["LIBELLE_TYPE_SUPPORT"]=="FOND EUROS",F.lit('EURO'))
            .otherwise(table_PM_début['LIBELLE_TYPE_SUPPORT'])
           )
.select("NUMERO_CONTRAT","PM_SUPPORT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT',"LIBELLE_CONVENTION_GESTION",'LIBELLE_PROFIL_GESTION','CODE_PROFIL','SUPPORT_SPECIFIQUE')
            .withColumnRenamed("PM_SUPPORT","PM_DEBUT")
           )

Base_PM=(table_PM_1.join(table_PM_2,["NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT',"LIBELLE_CONVENTION_GESTION",'LIBELLE_PROFIL_GESTION','CODE_PROFIL','SUPPORT_SPECIFIQUE'],how="full" )
         .groupby(["NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT',"LIBELLE_CONVENTION_GESTION",'LIBELLE_PROFIL_GESTION','CODE_PROFIL','SUPPORT_SPECIFIQUE',"DATE_VALEUR_UC","VALEUR_UC","LIBELLE_SUPPORT"])
         .agg(
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN")
         )
    .na.fill(value=0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Finale NOVA

# COMMAND ----------

window = Window.partitionBy("NUMERO_CONTRAT")

Base_finale = (Base_Flux_Finale.join(Base_PM,["NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT',"LIBELLE_CONVENTION_GESTION",'LIBELLE_PROFIL_GESTION','CODE_PROFIL','SUPPORT_SPECIFIQUE'],how='full')
                            .groupBy("NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT','LIBELLE_CONVENTION_GESTION','CODE_PROFIL','LIBELLE_PROFIL_GESTION','SUPPORT_SPECIFIQUE',"DATE_VALEUR_UC","LIBELLE_SUPPORT","VALEUR_UC")
                                              .agg(  
F.sum(round(F.col("Collecte_Brute_TOTALE"),2)).alias("Collecte_Brute_TOTALE"),
F.sum(round(F.col("Versement_Initial"),2)).alias("Versement_Initial"),  
F.sum(round(F.col("Versement_Complementaire"),2)).alias("Versement_Complementaire"),  
F.sum(round(F.col("Prelevement_Automatique"),2)).alias("Prelevement_Automatique"),
F.sum(round(F.col("Transfert_Entrant"),2)).alias("Transfert_Entrant"),


F.sum(round(F.col("Prestation_TOTALE"),2)).alias("Prestation_TOTALE"),                                    
F.sum(round(F.col("Rachat"),2)).alias("Rachat"),
F.sum(round(F.col("Sinistre"),2)).alias("Sinistre"), 
F.sum(round(F.col("Echu"),2)).alias("Echu"), 
F.sum(round(F.col("Transfert_Sortant"),2)).alias("Transfert_Sortant"),

                                   
F.sum(round(F.col("Arbitrage_Entrant"),2)).alias("Arbitrage_Entrant"),                                    
F.sum(round(F.col("Arbitrage_Entrant_RAG"),2)).alias("Arbitrage_Entrant_RAG"),
F.sum(round(F.col("Arbitrage_Entrant_Client"),2)).alias("Arbitrage_Entrant_Client"), 
F.sum(round(F.col("Arbitrage_Sortant"),2)).alias("Arbitrage_Sortant"), 
F.sum(round(F.col("Arbitrage_Sortant_RAG"),2)).alias("Arbitrage_Sortant_RAG"),
F.sum(round(F.col("Arbitrage_Sortant_Client"),2)).alias("Arbitrage_Sortant_Client"),

 F.sum(F.col("Frais_de_gestion")).alias("Frais_de_gestion"),
F.sum(F.col("Participation_aux_benefices")).alias("Participation_aux_benefices"),      

F.sum(round(F.col("PM_DEBUT"),2)).alias("PM_DEBUT"),                                    
F.sum(round(F.col("PM_FIN"),2)).alias("PM_FIN")                                                
                                   
                                 ).na.fill(value=0) 


#bloc pour supprimer les contrats sans informations qui n'ont aucun flux et ou PM sur l'année d'étude            
.withColumn("CUMUL", F.col('Collecte_Brute_TOTALE')+F.col('Prestation_TOTALE')+F.col('Arbitrage_Entrant')+F.col('Arbitrage_Sortant')+F.col('Frais_de_gestion')+F.col('Participation_aux_benefices')+F.col('PM_DEBUT')+F.col('PM_FIN')
           )
.withColumn("CUMUL_1", F.sum(F.col("CUMUL")).over(window))
.filter(F.col('CUMUL_1')!=0)
.drop("CUMUL","CUMUL_1")

#"NUMERO_CONTRAT","CODE_UT","CODE_UC","CODE_ISIN",'TYPE_SUPPORT','LIBELLE_CONVENTION_GESTION','CODE_PROFIL','LIBELLE_PROFIL_GESTION','SUPPORT_SPECIFIQUE',"DATE_VALEUR_UC","LIBELLE_SUPPORT","VALEUR_UC"



                               )

# COMMAND ----------

Base_NOVA_VF= (table_CONTRAT_finale.join(Base_finale,["NUMERO_CONTRAT"],how="left")

.withColumn("CODE_UT", F.when((F.col("CODE_UT")*1)>0,lpad(F.col("CODE_UT"),5,'0')).otherwise(F.col("CODE_UT")))
.withColumn("CODE_PRODUIT", F.when((F.col("CODE_UV")*1)>0,lpad(F.col("CODE_UV"),5,'0')).otherwise(F.col("CODE_UV")))

.join(Table_SEGMENT,["CODE_PRODUIT","CODE_UT"],how="left") 

.withColumn("SEGMENT",F.when(F.col("SEGMENT").isNull(),F.lit("Inconnu")).otherwise(F.col("SEGMENT")))

.withColumn("NUMERO_CONTRAT",lpad(F.col("NUMERO_CONTRAT"), 16, '0'))
.withColumn("CODE_PORTEFEUILLE",lpad(F.col("CODE_PORTEFEUILLE"), 10, '0'))
.withColumn("DT_VL_FIN",date_format(F.col("DATE_VALEUR_UC"),"dd/MM/yyyy"))
.withColumnRenamed("PROFESSION","LIBELLE_CSP")

.withColumnRenamed("LIBELLE_SUPPORT","NOM_SUPPORT")
.withColumnRenamed("LIBELLE_CONVENTION_GESTION","TYPE_GESTION")
.withColumnRenamed("LIBELLE_PROFIL_GESTION","PROFIL_GESTION")
.withColumn("SYSTEME",F.lit("NOVA"))
.join(personne_N0,["NUMERO_CLIENT","CODE_PORTEFEUILLE"],how="left")  
 
.filter( ~ ((  F.col("TYPE_SUPPORT").isNull() ) & ( (F.col("Collecte_Brute_TOTALE")==0) & (F.col("Prestation_TOTALE")==0) & (F.col("Arbitrage_Entrant")==0)  & (F.col("Arbitrage_Sortant")==0)  & (F.col("PM_DEBUT")==0)  & (F.col("PM_FIN")==0)                                ) )       )  
.filter( ~ ((  F.col("TYPE_PRSN").isNull() ) & ( (F.col("Collecte_Brute_TOTALE")==0) & (F.col("Prestation_TOTALE")==0) & (F.col("Arbitrage_Entrant")==0)  & (F.col("Arbitrage_Sortant")==0)  & (F.col("PM_DEBUT")==0)  & (F.col("PM_FIN")==0)                                ) )       )  

  ) 


# COMMAND ----------

Base_NOVA =(Base_NOVA_VF
.select(F.col("NUMERO_CONTRAT"),F.col("CODE_PRODUIT"),F.col("LIBELLE_PRODUIT").alias("NOM_PRODUIT"),F.col("LIBELLE_ETAT_CONTRAT").alias("SITUATION_CLOTURE"),
F.col("DATE_DEBUT_EFFET_CONTRAT").alias("DATE_EFFET"),F.col("DATE_FIN_EFFET_CONTRAT").alias("DATE_TERME"),F.col("DT_VL_FIN"),F.col("CODE_ISIN"),F.col("NOM_SUPPORT"),
F.col("SUPPORT_SPECIFIQUE"),F.col("CODE_UT"),F.col("TYPE_SUPPORT"),F.col("TYPE_GESTION"),F.col("PROFIL_GESTION"),F.col("Collecte_Brute_TOTALE"),F.col("Versement_Initial"),F.col("Versement_Complementaire"),F.col("Prelevement_Automatique"),F.col("Transfert_Entrant"),F.col("Prestation_TOTALE"),F.col("Rachat"),F.col("Sinistre"),F.col("Echu"),F.col("Transfert_Sortant"),F.col("Arbitrage_Entrant"),F.col("Arbitrage_Entrant_RAG"),F.col("Arbitrage_Entrant_Client"),F.col("Arbitrage_Sortant"),F.col("Arbitrage_Sortant_RAG"),F.col("Arbitrage_Sortant_Client"),F.col("PM_DEBUT"),F.col("PM_FIN"),F.col("CODE_PORTEFEUILLE"),F.col("UJ"),F.col("NUMERO_CLIENT"),F.col("TYPE_PRSN"),F.col("SYSTEME"),F.col("SEGMENT"),F.col("TOP_RETRAITE"),F.col("LB_CANAL")))

# COMMAND ----------

Base_NOVA.write.format("parquet").mode("overwrite").saveAsTable("Base_NOVA_VF")
