# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : Ludovic RAVENACH")
print("Date de création : 27/01/2024")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

pip install jours-feries-france

# COMMAND ----------

# MAGIC %run ./PARAMETRES 

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import *
from datetime import date
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
import calendar
from axapy.labs.piper import AgipiReader,DmtlsReader,RduReader
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
import pandas as pd

AgipiReader = AgipiReader()
DmtlsReader = DmtlsReader()
RduReader = RduReader()

# COMMAND ----------

# Fonction pour vérifier si un jour est un jour ouvré (lundi à vendredi)
annee_N_1= str((int(annee)-1))

def est_jour_ouvre(date):
    return date.weekday() < 6

# Déterminer la date de fin de l'année actuelle
fin_annee = datetime(int(annee_N_1), 12, 31)

# Compter les jours ouvrés à partir de la fin de l'année
jours_ouvres_restants = 4
date_cible = fin_annee
while jours_ouvres_restants > 0:
    date_cible -= timedelta(days=1)
    if est_jour_ouvre(date_cible):
        jours_ouvres_restants -= 1

periode_debut_agipi=date_cible.strftime("%Y-%m-%d")

# COMMAND ----------

annee_N_1= str((int(annee)-1))
jour=str(calendar.monthrange(int(annee), int(mois))[1])
test_date = date(int(annee),int(mois),calendar.monthrange(int(annee), int(mois))[1])

from jours_feries_france import JoursFeries
res = JoursFeries.for_year(int(annee)) # une liste 

df1=pd.DataFrame(list(res.items()),columns=['Name', 'Date'])
df2 = pd.DataFrame([["Jour de l'an",""+annee+"-12-31"]], columns=['Name','Date'])
df2['Date'] = pd.to_datetime(df2['Date']).dt.strftime('%Y-%m-%d')
df1=(spark.createDataFrame(df1)) # pandas => pyspark
df2=(spark.createDataFrame(df2))# pandas => pyspark
df=df1.union(df2)

s=(df

.withColumn("nomj",date_format("Date", "EEEE"))
  .filter((F.col("nomj")!="Sunday"))
  .filter((F.col("nomj")!="Saturday"))
  .filter(date_format("Date", "dd")>25)
  .filter(date_format("Date", "MM")==mois)
.drop("Name","nomj")      
.withColumn("mois",date_format("Date", "MM"))
.groupBy("mois").agg(F.count(F.col("mois")).alias("nbjf"))
.drop("mois"))

s=s.select(s.nbjf).rdd.flatMap(lambda x: x).collect() #dataframe => list

if len(s)==1:
  s=s[0]
else: s=0


diff=0
for k in [0,1,2,3,4,5,6]:
  if test_date.weekday() == 6:
      diff = 5 + s
  elif test_date.weekday() == 5:
      diff = 4 + s
  elif test_date.weekday() == 4:
      diff = 3 + s
  elif test_date.weekday() == 3:
      diff = 3 + s
  elif test_date.weekday() == 2:
      diff = 5 + s
  elif test_date.weekday() == 1:
      diff = 5 + s
  elif test_date.weekday() == 0:
      diff = 5 + s
  
  date_fin = str(test_date - timedelta(days=diff))


Periode = [""+annee_N_1 +"-01-01",periode_debut_agipi,""+ annee_N_1 +"-12-31",""+annee +"-01-01",date_fin,""+annee+"-"+mois+"-"+jour+""]  


# COMMAND ----------

Transactions = ["REORIENTATION D'EPARGNE", "RACHAT PARTIEL", "RACHAT TOTAL", "VERSEMENT"]
liste_supports_spé = ["FR0013218302", "FR0011129717","FR0013076031", "FR0013466562", "FR0013473543","FR001400G529","FR0000288946","FR001400IO08","FR1459AB1751","FR2CIBFS6232"]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE PM

# COMMAND ----------


windowSpec  = Window.partitionBy("NUMERO_ADHESION_AXA","CODE_UT").orderBy(F.col("DATE_VALORISATION").desc())

 #Récupération des PM de l'année N

PM_N = ( DmtlsReader.read_table('ag_adhesion_valorisation')
.select( lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias('NUMERO_ADHESION_AXA'), F.col("CODE_ISIN"),F.col("CODE_SUPPORT"), F.col("CODE_UT"), F.col("LIBELLE_SUPPORT") ,F.col("CODE_TYPE_CONTRAT"),F.col("MONTANT"), F.col("DATE_VALORISATION"),
F.col("CODE_TYPE_CONTRAT")).distinct()
.withColumn("CODE_ISIN",F.when(F.col("CODE_ISIN").isNull(),"ND").otherwise(F.col("CODE_ISIN")))
  #.filter((F.col("DATE_VALORISATION")>=Periode[1])&(F.col("DATE_VALORISATION")<=Periode[3]))

    .filter((F.col("DATE_VALORISATION")<=Periode[5]))    
    .filter(F.col("LIBELLE_TYPE_MONTANT")=="Montant de l'épargne gérée")

.withColumn("TOP_LAST",dense_rank().over(windowSpec)).filter(F.col("TOP_LAST")=="1").drop("TOP_LAST")
.join((DmtlsReader.read_table('ag_adhesion_epargne')
.select(lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias('NUMERO_ADHESION_AXA'),F.col("STATUT"))
.filter(F.col("STATUT")=="En cours")),["NUMERO_ADHESION_AXA"],how="inner")

.withColumnRenamed("MONTANT","PM_FIN")
.join(DmtlsReader.read_table('ag_catalogue_produit_support').select(F.col("CODE_SUPPORT"),F.col("CODE_TYPE_CONTRAT"),F.col("CODE_UT"),F.col("CODE_ISIN"),F.col("CODE_UV")).withColumn("CODE_ISIN",F.when(F.col("CODE_ISIN").isNull(),"ND").otherwise(F.col("CODE_ISIN")))
 ,["CODE_SUPPORT","CODE_TYPE_CONTRAT","CODE_UT","CODE_ISIN"] ,how="left"   )

.groupBy(F.col("NUMERO_ADHESION_AXA"),F.col("CODE_ISIN"),F.col("CODE_SUPPORT"),F.col("CODE_UT"),F.col("CODE_UV")
,F.col("LIBELLE_SUPPORT"),F.col("DATE_VALORISATION"))
.agg(F.sum(F.col("PM_FIN")).alias("PM_FIN"))  
     ).sort(["NUMERO_ADHESION_AXA"],descending=True) 

               

#Récupération des PM de l'année N-1                 

PM_N_1 = ( DmtlsReader.read_table('ag_adhesion_valorisation')
          
.select(  lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias('NUMERO_ADHESION_AXA'), F.col("CODE_ISIN"),F.col("CODE_SUPPORT"), F.col("CODE_UT"), F.col("LIBELLE_SUPPORT") ,F.col("MONTANT"), F.col("DATE_VALORISATION")               )
.withColumn("CODE_ISIN",F.when(F.col("CODE_ISIN").isNull(),"ND").otherwise(F.col("CODE_ISIN")))
    #.filter((F.col("DATE_VALORISATION")>=Periode[0])&(F.col("DATE_VALORISATION")<=Periode[1]))
    .filter((F.col("DATE_VALORISATION")<=Periode[2]))    
    .filter(F.col("LIBELLE_TYPE_MONTANT")=="Montant de l'épargne gérée")

.withColumn("TOP_LAST",dense_rank().over(windowSpec)).filter(F.col("TOP_LAST")=="1").drop("TOP_LAST")
.join((DmtlsReader.read_table('ag_adhesion_epargne')
.select(lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias('NUMERO_ADHESION_AXA'),F.col("STATUT"))
.filter(F.col("STATUT")=="En cours")),["NUMERO_ADHESION_AXA"],how="inner")

.withColumnRenamed("MONTANT","PM_DEBUT")
.groupBy(F.col("NUMERO_ADHESION_AXA"),F.col("CODE_ISIN"),F.col("CODE_SUPPORT"),F.col("CODE_UT"),F.col("LIBELLE_SUPPORT"))
.agg(F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT")) ).sort(["NUMERO_ADHESION_AXA"],descending=True) 



#Création de la base des PM de l'année

Base_PM = ( PM_N.join(PM_N_1,["NUMERO_ADHESION_AXA","CODE_ISIN","CODE_SUPPORT","CODE_UT","LIBELLE_SUPPORT"],how="full" )
.join(

(DmtlsReader.read_table("ag_catalogue_support",archive=True)
 .withColumn("TYPE_SUPPORT",F.when(F.col("LIBELLE_NATURE_SUPPORT")=="Part","UC")
 .when( F.col("LIBELLE_NATURE_SUPPORT")=="Capitaux","EURO")
 .otherwise(F.lit("EUROCROISSANCE")) )
.select(F.col("CODE_SUPPORT"),F.col("TYPE_SUPPORT")          )                
            
            )
,[ "CODE_SUPPORT" ],how="left")
.groupby(["NUMERO_ADHESION_AXA","CODE_ISIN","CODE_SUPPORT","CODE_UT","CODE_UV","TYPE_SUPPORT","DATE_VALORISATION"])
 .agg(
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN")
         ).na.fill(value=0)
.withColumn("NUMERO_CONTRAT",lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0'))
.drop("NUMERO_ADHESION_AXA")


.withColumn("DT_VL_FIN",date_format(F.col("DATE_VALORISATION"),"dd/MM/yyyy"))



)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE FLUX

# COMMAND ----------

Base_Mvt_dtl = (DmtlsReader.read_table('ag_operation_epargne_detail')

.filter(F.col("LIBELLE_TYPE_TRANSACTION").isin(Transactions))
                                  .select((lpad("NUMERO_ADHESION_AXA", 16, '0').alias("NUMERO_ADHESION_AXA")), 
                                          "NUMERO_ADHESION", "NUMERO_OPERATION", "DATE_RECEPTION", "CODE_ISIN", "LIBELLE_SUPPORT","CODE_SUPPORT","CODE_UT","LIBELLE_NATURE_SUPPORT","FLAG_PROVISOIRE","FLAG_ANNULATION",
                                          "LIBELLE_CATEGORIE_SUPPORT", "LIBELLE_TYPE_TRANSACTION", "DETAIL_TRANSACTION", "MONTANT", "FLAG_SUPPORT_CONVENTION","CODE_CONVENTION_GESTION",
                                          "CATEGORIE_TYPE_MONTANT")
                                  .drop_duplicates(["NUMERO_ADHESION", "NUMERO_OPERATION","CODE_ISIN", "LIBELLE_SUPPORT","CATEGORIE_TYPE_MONTANT"])

.withColumn('TYPE_SUPPORT',
                        F.when((F.col("LIBELLE_NATURE_SUPPORT")=="Capitaux"), "EURO")
                        .when((F.col("LIBELLE_NATURE_SUPPORT")=="Part"), "UC")
                             .otherwise("EUROCROISSANCE")  )
                       # .when((F.col("LIBELLE_NATURE_SUPPORT")=="Euro Croissance"), "EUROCROISSANCE")
                       # .otherwise("AUTRE")  )



.join(DmtlsReader.read_table('ag_operation_epargne')
                                    .select("NUMERO_ADHESION_AXA","NUMERO_OPERATION","NUMERO_ADHESION","DATE_JOURNALIER","CODE_UV","LIBELLE_PAIEMENT")
                                    .distinct()
                                    .filter(F.col("LIBELLE_TYPE_TRANSACTION").isin(Transactions))
                                    .filter((F.col("DATE_JOURNALIER")>=Periode[1])&(F.col("DATE_JOURNALIER")<=Periode[4]))

                                  ,["NUMERO_ADHESION_AXA","NUMERO_OPERATION","NUMERO_ADHESION"],how="right")  

   .withColumn("Type_Transaction",
                        # VERSEMENTS
                        when(F.col("LIBELLE_TYPE_TRANSACTION")=="VERSEMENT",
                             when(F.col("DETAIL_TRANSACTION").isin("Versement initial", "Annulation : Versement initial","Versement initial investissement progressif", "Annulation : Versement initial investissement progressif","Versement initial transfert","Annulation : Versement initial transfert"), "VI")
                             # Transferts Entrants
                             .when(F.col("DETAIL_TRANSACTION").isin("Versement transfert","Annulation : Versement transfert"), "TE")
                             # Versements automatiques
                             .when(F.col("LIBELLE_PAIEMENT")=="PRELEVEMENT", "PA")
                             # Versements complémentaires
                             .otherwise("VC"))
                        
                        # RETRAITS
                        .when(F.col("LIBELLE_TYPE_TRANSACTION").isin('RACHAT PARTIEL', 'RACHAT TOTAL'),
                             # Rachats
                             when(  (   F.col("DETAIL_TRANSACTION").isin(["Rachat anticipé","Rachat anticipé Achat Résidence principale","Rachat COVID","Rachat partiel","Rachat partiel programmé","Rachat total","Annulation : Rachat anticipé","Annulation : Rachat anticipé Achat Résidence principale","Annulation : Rachat COVID","Annulation : Rachat partiel","Annulation : Rachat partiel programmé","Annulation : Rachat total"])),"RT")
                             # décès
                             .when(     F.col("DETAIL_TRANSACTION").contains("décès")   ,"SI") 
                             # Echus
                             .when(  (F.col("DETAIL_TRANSACTION").isin(["Reprise Conversion en rente","Conversion en rente","Versement du capital à la Caisse des Dépôts et Consignations","Restitution en capital","Annulation : Conversion en rente","Annulation : Reprise Conversion en rente","Annulyation : Versement du capital à la Caisse des Dépôts et Consignations","Annulation : Restitution en capital"])),"EC")
                             # Transferts
                             .when( F.col("DETAIL_TRANSACTION").contains("Transfert"),"TS"    )
                             .otherwise("AR"))
                             
  
                        # ARBITRAGES
                        .when(F.col("LIBELLE_TYPE_TRANSACTION")=="REORIENTATION D'EPARGNE",
                             
                              when(
                               ((~F.col("DETAIL_TRANSACTION").contains("Annulation :")) & (F.col('MONTANT')>=0))
                               | ((F.col("DETAIL_TRANSACTION").contains("Annulation :"))  & (F.col('MONTANT')<0))
                                  
                                  ,"AE")
                             
                              .when(
                               ((~F.col("DETAIL_TRANSACTION").contains("Annulation :")) & (F.col('MONTANT')<0))
                               | ((F.col("DETAIL_TRANSACTION").contains("Annulation :")) & (F.col('MONTANT')>=0))
                                   
                                   , "AS") 
                        
                              .otherwise("AUTRE"))
   )
.withColumn("MONTANT_TRANSACTION",
                        when(
                           (F.col("Type_Transaction").isin(["AE","AS"])) & (F.col("CATEGORIE_TYPE_MONTANT").isin(["MONTANT_NET","FRAIS_ARBITRAGE"])),
                             F.col("MONTANT")) 
                        
                        .when( 
                               (~F.col("Type_Transaction").isin(["AE","AS"])) & (F.col("CATEGORIE_TYPE_MONTANT").isin(["MONTANT_BRUT"])) ,
                              F.col("MONTANT"))
                        .otherwise(0))

                             
      # Collecte   
               
.withColumn("Versement_Initial", F.when( F.col("Type_Transaction")=="VI"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Versement_Complementaire", F.when( F.col("Type_Transaction")=="VC"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Transfert_Entrant", F.when( F.col("Type_Transaction")=="TE"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Prelevement_Automatique", F.when( F.col("Type_Transaction")=="PA"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Collecte_Brute_TOTALE", F.col("Versement_Initial")+F.col("Versement_Complementaire")+F.col("Transfert_Entrant")+F.col("Prelevement_Automatique"))  


     # Arbitrages                  
               
.withColumn("Arbitrage_Entrant", F.when( F.col("Type_Transaction")=="AE"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))

.withColumn("Arbitrage_Entrant_RAG", 
            F.when( 
                   ( (F.col("Type_Transaction")=="AE") & (F.col("DETAIL_TRANSACTION").isin(["Ajustement semestriel gratuit","Ajustement annuel gratuit","Réorientation en gestion pilotée","Rééquilibrage continu gratuit"]) ) & (F.col("MONTANT_TRANSACTION")>=0) ) 
                   | ( (F.col("Type_Transaction")=="AE") &   (F.col("DETAIL_TRANSACTION").isin(["Annulation : Ajustement semestriel gratuit","Annulation : Ajustement annuel gratuit","Annulation : Réorientation en gestion pilotée","Annulation : Rééquilibrage continu gratuit"]) ) & (F.col("MONTANT_TRANSACTION")<0) ) 
                    
                   , F.col("MONTANT_TRANSACTION")
                   ).otherwise(0)
            ) 

.withColumn('Arbitrage_Entrant_Client', F.col('Arbitrage_Entrant')-F.col('Arbitrage_Entrant_RAG'))  


.withColumn("Arbitrage_Sortant", F.when( F.col("Type_Transaction")=="AS",F.col("MONTANT_TRANSACTION")).otherwise(0))

.withColumn("Arbitrage_Sortant_RAG",
             F.when(  
                    ( (F.col("Type_Transaction")=="AS") &   (F.col("DETAIL_TRANSACTION").isin(["Ajustement semestriel gratuit","Ajustement annuel gratuit","Réorientation en gestion pilotée","Rééquilibrage continu gratuit"]) ) & (F.col("MONTANT_TRANSACTION")<0) &  (F.col("CATEGORIE_TYPE_MONTANT").isin(["MONTANT_NET","FRAIS_ARBITRAGE"])) ) 
                    | ( (F.col("Type_Transaction")=="AS") &   (F.col("DETAIL_TRANSACTION").isin(["Annulation : Ajustement semestriel gratuit","Annulation : Ajustement annuel gratuit","Annulation : Réorientation en gestion pilotée","Annulation : Rééquilibrage continu gratuit"]) ) & (F.col("MONTANT_TRANSACTION")>=0) &   (F.col("CATEGORIE_TYPE_MONTANT").isin(["MONTANT_NET","FRAIS_ARBITRAGE"])) )
                         
                    , F.col("MONTANT_TRANSACTION")
                    ).otherwise(0)
             ) 
 .withColumn('Arbitrage_Sortant_Client', F.col('Arbitrage_Sortant')-F.col('Arbitrage_Sortant_RAG'))    


    # Prestations    


.withColumn("Rachat", F.when( F.col("Type_Transaction").isin(["RT","AR"])    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Sinistre", F.when( F.col("Type_Transaction")=="SI"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Echu", F.when( F.col("Type_Transaction")=="EC"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Transfert_Sortant", F.when( F.col("Type_Transaction")=="TS"    ,   F.col("MONTANT_TRANSACTION")).otherwise(0))
.withColumn("Prestation_TOTALE", F.col("Rachat")+ F.col("Sinistre")+ F.col("Echu")+ F.col("Transfert_Sortant") )


    # Autres    

.withColumn("Frais_Versement", 
            F.when(  
                   (F.col("LIBELLE_TYPE_TRANSACTION").isin(["VERSEMENT"]))  &  ( F.col("CATEGORIE_TYPE_MONTANT").isin(["FRAIS_ADHESION","FRAIS_FIXES","FRAIS_VARIABLES"]) ) & (F.col("MONTANT")>0)
                     
                   ,F.col("MONTANT")
                   ).otherwise(0)
            )
.withColumn("Frais_Arbitrage",
             F.when( 
                      (F.col("LIBELLE_TYPE_TRANSACTION").isin(["REORIENTATION D'EPARGNE"]))  &  ( F.col("CATEGORIE_TYPE_MONTANT").isin(["FRAIS_ARBITRAGE"]) )  & (F.col("MONTANT")>0) 
                      ,F.col("MONTANT")
                      ).otherwise(0)
             )
.withColumn("Frais_Rachat",
             F.when(   
                    (F.col("LIBELLE_TYPE_TRANSACTION").isin(["RACHAT TOTAL","RACHAT PARTIEL"]))  &  ( F.col("CATEGORIE_TYPE_MONTANT").isin(["FRAIS_GESTION"]) ) & (F.col("MONTANT")>0)         
                    ,F.col("MONTANT")
                    ).otherwise(0)
             )     

 .na.fill(value=0)   

   )


window = Window.partitionBy("NUMERO_CONTRAT")

Base_Flux_Finale=(Base_Mvt_dtl.groupBy(["NUMERO_ADHESION_AXA","CODE_ISIN","CODE_SUPPORT","CODE_UT","TYPE_SUPPORT","CODE_CONVENTION_GESTION","LIBELLE_SUPPORT","CODE_UV"])
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
F.sum(F.col("Arbitrage_Sortant")*-1).alias("Arbitrage_Sortant"), 
F.sum(F.col("Arbitrage_Sortant_RAG")*-1).alias("Arbitrage_Sortant_RAG"),
F.sum(F.col("Arbitrage_Sortant_Client")*-1).alias("Arbitrage_Sortant_Client"),                                   
                                   
F.sum(F.col("Frais_Versement")).alias("Frais_Versement"),                                    
F.sum(F.col("Frais_Arbitrage")).alias("Frais_Arbitrage"),
F.sum(F.col("Frais_Rachat")).alias("Frais_Rachat")
                                 ).na.fill(value=0) 

.withColumn("NUMERO_CONTRAT",lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0'))
.drop("NUMERO_ADHESION_AXA")

.withColumn("CUMUL", F.col('Collecte_Brute_TOTALE')+F.col('Prestation_TOTALE')+F.col('Arbitrage_Entrant')+F.col('Arbitrage_Sortant')+F.col('Frais_Rachat')+F.col('Frais_Arbitrage')+F.col('Frais_Versement')
           )
.withColumn("CUMUL_1", F.sum(F.col("CUMUL")).over(window))
.filter(F.col('CUMUL_1')!=0)
.drop("CUMUL","CUMUL_1")
.withColumn("CODE_ISIN",F.when(F.col("CODE_ISIN").isNull(),"ND").otherwise(F.col("CODE_ISIN")))
                               )                
                  



# COMMAND ----------

window = Window.partitionBy("NUMERO_CONTRAT")

Base_PM_Flux=(Base_Flux_Finale.join(Base_PM,["NUMERO_CONTRAT","CODE_ISIN","CODE_SUPPORT","CODE_UT","CODE_UV","TYPE_SUPPORT"],how="full")
               .groupBy(["NUMERO_CONTRAT","CODE_ISIN","CODE_SUPPORT","CODE_UT","TYPE_SUPPORT","CODE_CONVENTION_GESTION","CODE_UV","LIBELLE_SUPPORT","DT_VL_FIN"])
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
                                   
F.sum(F.col("Frais_Versement")).alias("Frais_Versement"),                                    
F.sum(F.col("Frais_Arbitrage")).alias("Frais_Arbitrage"),
F.sum(F.col("Frais_Rachat")).alias("Frais_Rachat"),
                                                 
F.sum(F.col("PM_DEBUT")).alias("PM_DEBUT"),
F.sum(F.col("PM_FIN")).alias("PM_FIN") ).na.fill(value=0)              
  
             
 #bloc pour supprimer les contrats sans informations qui n'ont aucun flux et ou PM sur l'année d'étude            
.withColumn("CUMUL", F.col('Collecte_Brute_TOTALE')+F.col('Prestation_TOTALE')+F.col('Arbitrage_Entrant')+F.col('Arbitrage_Sortant')+F.col('Frais_Rachat')+F.col('Frais_Arbitrage')+F.col('Frais_Versement')+F.col('PM_DEBUT')+F.col('PM_FIN')
           )
.withColumn("CUMUL_1", F.sum(F.col("CUMUL")).over(window))
.filter(F.col('CUMUL_1')!=0)
.drop("CUMUL","CUMUL_1")

             )


# COMMAND ----------

# MAGIC %md
# MAGIC ##### BASE INFO = Souscripteurs / Contrats / Gestions / Produits

# COMMAND ----------

Base_Souscripteur=(DmtlsReader.read_table('ag_personne',archive=True)

#.join(
#(AgipiReader.read_table('tr_personne').select(F.col("NUMERO_PERSONNE"),F.col("NUMERO_SIREN"),F.col("NUMERO_SIRET")             )
#.dropDuplicates(["NUMERO_PERSONNE"])),["NUMERO_PERSONNE"],how="left"  )
.drop("CODE_POSTAL")
.withColumnRenamed("NUMERO_PERSONNE","NUMERO_CLIENT")
.withColumnRenamed("CODE_PROFESSION","CODE_CSP")
.withColumnRenamed("LIBELLE_PROFESSION","LIBELLE_CSP")
.withColumnRenamed("LIBELLE_PAYS","PAYS")
.withColumnRenamed("CODE_INSEE_PAYS","CODE_POSTAL")
.withColumnRenamed("ADRESSE_EMAIL","EMAIL")
.withColumnRenamed("LIGNE_ADRESSE_1","ADRESSE")
#.withColumnRenamed("NUMERO_SIREN","SIREN")
#.withColumnRenamed("NUMERO_SIRET","SIRET")   


.withColumn("TYPE_PRSN", F.when(   F.col("LIBELLE_TYPE_PERSONNE")=="Personne physique",  F.lit("PHY")     ).otherwise(F.lit("MOR")))
.withColumn("CIVILITE", F.when(   F.col("LIBELLE_POLITESSE")=="Madame",  F.lit("Mme")     )
.when(   F.col("LIBELLE_POLITESSE")=="Monsieur",  F.lit("M.")     ).otherwise(F.col("LIBELLE_TYPE_SOCIETE")))
.withColumn( "NOM_PRENOM_SOUSCRIPTEUR" ,   F.concat_ws(" ",F.col("NOM_CLIENT"),F.col("PRENOM_CLIENT"))             )


.select(lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias("NUMERO_CONTRAT"), F.col("NUMERO_CLIENT"),F.col("CIVILITE"),F.col("NOM_PRENOM_SOUSCRIPTEUR"),F.col("TYPE_PRSN"),date_format(F.col("DATE_NAISSANCE"),"dd/MM/yyyy").alias("DATE_NAISSANCE"), F.col("ADRESSE"),F.col("CODE_POSTAL"), F.col("VILLE") , F.col("PAYS") ,F.col("EMAIL"),F.col("CODE_CSP"),F.col("LIBELLE_CSP") )
#,F.col("SIREN"),F.col("SIRET")
).dropDuplicates(["NUMERO_CONTRAT"])

# Dans le niveau 1 la variable LIBELLE_ENTITE_JURIDIQUE des données manquantes 

#UJ_N0=(AgipiReader.read_table('tj_adhesion_entite_juridique')
#.select(  F.col("ADHESION_NUMERO").alias("NUMERO_ADHESION"),F.col("ENTITE_JURIDIQUE_CODE"))
#.withColumn("UJ", F.when(   F.col("ENTITE_JURIDIQUE_CODE").isin(["AS"]),  F.lit("SA")     )
#.when(F.col("ENTITE_JURIDIQUE_CODE").isin(["AM","XM"]),  F.lit("MU") )
#.otherwise(F.lit("ND")))
#.drop("ENTITE_JURIDIQUE_CODE")


#).dropDuplicates(["NUMERO_ADHESION"])


Base_Contrat_Gestion_Produit=(DmtlsReader.read_table('ag_adhesion_epargne')

.join(DmtlsReader.read_table("ag_convention_gestion").select(F.col("CODE_CONVENTION_GESTION"),F.col("LIBELLE_THEMATIQUE_CONVENTION"),
F.col("LIBELLE_PROFIL_FORMULE_GESTION").alias("PROFIL_GESTION") )          ,["CODE_CONVENTION_GESTION"],how="left").distinct() 


.withColumn("DATE_EFFET",date_format("DATE_DEBUT_EFFET_CONTRAT","dd/MM/yyyy"))
.withColumn("DATE_TERME",date_format("DATE_ANNULATION","dd/MM/yyyy"))

.withColumnRenamed("STATUT","SITUATION_CLOTURE")
.withColumnRenamed("LIBELLE_PRODUIT","NOM_PRODUIT")
#.drop("LIBELLE_ENTITE_JURIDIQUE")
.withColumn("UJ", F.when(   F.col("LIBELLE_ENTITE_JURIDIQUE")=="AXA France Vie",  F.lit("SA")     )
.when( F.col("LIBELLE_ENTITE_JURIDIQUE")=="AXA Assurances Vie Mutuelle",  F.lit("MU")   )           
.otherwise(F.lit("ND")))
#.join(UJ_N0,["NUMERO_ADHESION"],how="full")
.select(lpad(F.col("NUMERO_ADHESION_AXA"), 16, '0').alias('NUMERO_CONTRAT'),F.col("NUMERO_ADHESION"),lpad(F.col("CODE_PORTEFEUILLE"), 10, '0').alias('CODE_PORTEFEUILLE'),F.col("DATE_EFFET"),F.col("DATE_TERME"),F.col("SITUATION_CLOTURE"),F.col("NOM_PRODUIT"),F.col("UJ"),F.col("LIBELLE_CONVENTION_GESTION").alias("TYPE_GESTION"),F.col("CANAL_PORTEFEUILLE").alias("LB_CANAL"),F.col("LIBELLE_TYPE_CONTRAT"),
F.col("LIBELLE_THEMATIQUE_CONVENTION"),F.col("PROFIL_GESTION"))

 )


# COMMAND ----------

DT_VL=(PM_N.filter(F.col("DATE_VALORISATION").isNotNull())
.orderBy(F.col("DATE_VALORISATION").desc())
.select(F.first(F.col("DATE_VALORISATION")).alias("DATE_VALORISATION"))
.distinct())
#DT_VL=DT_VL.select(DT_VL.DATE_VALORISATION).rdd.flatMap(lambda x: x).collect()

Base_AGIPI_VF=(Base_PM_Flux.join(Base_Souscripteur.join(Base_Contrat_Gestion_Produit,["NUMERO_CONTRAT"],how="left"),["NUMERO_CONTRAT"],how="left")
 .withColumn('SUPPORT_SPECIFIQUE',
                        when((F.col("CODE_ISIN").isin(liste_supports_spé)), "Oui")
                        .otherwise("Non")) 

.withColumn("CODE_UT", F.when((F.col("CODE_UT")*1)>0,lpad(F.col("CODE_UT"),5,'0')).otherwise(F.col("CODE_UT")))
.withColumn("CODE_PRODUIT", F.when((F.col("CODE_UV")*1)>0,lpad(F.col("CODE_UV"),5,'0')).otherwise(F.col("CODE_UV")))                        

 #.join(Table_SEGMENT,["CODE_UT","CODE_UV"],how="left")                        

.withColumn("SEGMENT",F.lit("Agipi"))
.withColumn("SYSTEME", F.lit("AGIPI"))
.withColumnRenamed("LIBELLE_SUPPORT","NOM_SUPPORT")
#.filter(F.col("CODE_PORTEFEUILLE").isNotNull())
.withColumn('TOP_RETRAITE', when(F.col('LIBELLE_TYPE_CONTRAT').isin('FAR', 'PAIR','NOVIAL AVENIR'), 'RETRAITE').otherwise('EPARGNE'))
.withColumn("UJ",F.when(F.col("UJ").isNull(),F.lit("SA")).otherwise(F.col("UJ")))
.withColumn("LB_CANAL",F.when(F.col("LB_CANAL").isNull(),F.lit("ND")).otherwise(F.col("LB_CANAL")))
.withColumn("DT_VL_FIN",F.when(F.col("DT_VL_FIN").isNull(),str(Periode[5])).otherwise(F.col("DT_VL_FIN")))
)


# COMMAND ----------

Base_AGIPI=(Base_AGIPI_VF
.select(F.col("NUMERO_CONTRAT"),F.col("CODE_PRODUIT"),F.col("NOM_PRODUIT"),F.col("SITUATION_CLOTURE"),F.col("DATE_EFFET"),F.col("DATE_TERME"),F.col("DT_VL_FIN"),F.col("CODE_ISIN"),F.col("NOM_SUPPORT"),F.col("SUPPORT_SPECIFIQUE"),F.col("CODE_UT") ,F.col("TYPE_SUPPORT"),F.col("TYPE_GESTION"),F.col("PROFIL_GESTION"),F.col("Collecte_Brute_TOTALE")	,F.col("Versement_Initial")	,F.col("Versement_Complementaire"),F.col("Prelevement_Automatique"),F.col("Transfert_Entrant")	,F.col("Prestation_TOTALE")	,F.col("Rachat"),F.col("Sinistre"),F.col("Echu")	,F.col("Transfert_Sortant")	,F.col("Arbitrage_Entrant")	,F.col("Arbitrage_Entrant_RAG")	,F.col("Arbitrage_Entrant_Client")	,F.col("Arbitrage_Sortant")	,F.col("Arbitrage_Sortant_RAG")	,F.col("Arbitrage_Sortant_Client"),F.col("PM_DEBUT"),F.col("PM_FIN"),F.col("CODE_PORTEFEUILLE"),F.col("UJ"),F.col("NUMERO_CLIENT"),F.col("TYPE_PRSN"),F.col("SYSTEME"),F.col("SEGMENT"),F.col("TOP_RETRAITE"),F.col("LB_CANAL")))

# COMMAND ----------

Base_AGIPI.write.format("parquet").mode("overwrite").saveAsTable("Base_AGIPI_VF")
