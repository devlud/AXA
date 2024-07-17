# Databricks notebook source
dataframes=[]

# COMMAND ----------


# 1- On ne peut pas mettre un %RUN dans une boucle sur databricks 
# 2- On ne peut pas également mettre un dbutils.notebook.run() dans une boucle
# 3- Impossible aussi d'appeler une boucle dans un workflow
# => solution sale : On met tout le code dans une même fonction pour pouvoir utiliser une boucle sur cette fonction sur mois et annee
# (qui seront comme  paramètres de la fonction) => Databrciks raisonne sur des notebook et non en mode "class" ce qui rend les itérations compliquées 

def recup_AWS_RDU_et_initiative_UC(mois,annee):


    #############
    ## PARTIE AWS 
    #############


    import pyspark.sql.functions as F
    from pyspark.sql.functions import lpad
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
    
    Table_SEGMENT = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .load("dbfs:/FileStore/Power BI/cartographie_segments_revue.csv")\
    .withColumn("CODE_UT", F.when((F.col("CODE_UT")*1)>0,lpad(F.col("CODE_UT"),5,'0')).otherwise(F.col("CODE_UT")))\
    .withColumn("CODE_PRODUIT", F.when((F.col("CODE_UV")*1)>0,lpad(F.col("CODE_UV"),5,'0')).otherwise(F.col("CODE_UV")))\
    .select(F.col("CODE_UT"),F.initcap(F.col('SEGMENT')).alias("SEGMENT"),F.col("CODE_PRODUIT"))\
    .distinct()

    liste_supports_spé= ["FR0013076031", "FR0011129717", "FR0013466562", "FR0013473543", "FR0010188334", "FR0125508632", "FR0123426035", "FR0122689674", "FR0122540299", "FR0124236763", "FR0122338959", "IE00B8501520", "FR0122919402", "FR0122919428", "FR0123169676", " LU1719067172", "IE00BDBVWQ24", "IE00BHY3RV36", "FR001400CZ43"]
    
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
        .na.fill(value=0))

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

    window_bis = Window.partitionBy("ID_CONTRAT")

    Base_Contrat_VF=(
        Base_Contrat_Finale.union(Base_Contrat_Finale_archive)
        .withColumn("CUMUL", F.sum(F.col("TOP")).over(window_bis))
        .filter(((F.col("CUMUL") > 1) & (F.col('SITUATION_CLOTURE')!="En erreur"))|(F.col("CUMUL")==1))
        )
 
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


    #############
    ## PARTIE RDU 
    #############


    import pyspark.sql.functions as F
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
    F.col("KAGFGPTF_CANAL").alias("CANAL_RDU_PRINCIPAL"),F.col("KAGFGPTF_CD_RESEAU").alias("CD_RESEAU"),F.col("KAGFGPTF_CD_REGROUP").alias("CD_REGROUPEMENT"),F.col("KAGFGPTF_LIB_CANAL").alias("LIB_CANAL"),F.col("KAGFGPTF_ID_DISTR_1").alias("CODE_PCR")     ).dropDuplicates(["CODE_PORTEFEUILLE"])  
    .withColumn("CODE_PORTEFEUILLE",lpad(F.col("CODE_PORTEFEUILLE"), 10, '0'))

    )


    #############
    ## PARTIE INITIATIVE UC
    #############


    Base_data = (Base_AWS_VF.join(RDU,["CODE_PORTEFEUILLE"],how="left") )

    # A modifier le code isin à chaque année => Voir avec l'équipe de Marc Bressoud
    Fonds_Campagne=["FR0013466562","FR001400BZG1","FR0013473543","FR1459AB1751","FR001400CZ43","FR001400G529","FR001400IO08","FR2CIBFS6232"]

    Base_data_fds_campagne = (Base_data
                            .select(F.col("CODE_ISIN"),F.col("Collecte_Brute_TOTALE"),F.col("CANAL_RDU_PRINCIPAL"))
                            .filter(F.col("CODE_ISIN").isin(Fonds_Campagne)) 
                            .withColumn("Réseau", F.when(F.col("CANAL_RDU_PRINCIPAL").isin(["011","012","039"]),F.lit("4 : A2P"))
                                          .when(F.col("CANAL_RDU_PRINCIPAL").isin(["010","001","006","007","009","038","028"]),F.lit("1 : AGA"))
                                          .when(F.col("CANAL_RDU_PRINCIPAL").isin(["013","014"]),F.lit("3 : Salarié"))
                                          .otherwise(F.lit("Autre"))
                            )

                            .withColumn('FOND DE CAMPAGNE', 
                              when(F.col("CODE_ISIN").isin(["FR0013466562","FR001400BZG1"]), F.lit("AXA Avenir Infrastructure"))
                              .when(F.col("CODE_ISIN") == "FR0013473543", F.lit("AXA Avenir Entrepreneur"))

                              #composition de EMTN : AXA coupon opportunité, AXA coupon Ambition, AXA coupon Ambition Monde, AXA coupon Ambition Monde 2

                              .when(F.col("CODE_ISIN") == "FR1459AB1751", F.lit("AXA coupon opportunité"))
                              .when(F.col("CODE_ISIN") == "FR001400CZ43", F.lit("AXA coupon Ambition"))
                              .when(F.col("CODE_ISIN") == "FR001400G529", F.lit("AXA coupon Ambition Monde"))
                              .when(F.col("CODE_ISIN") == "FR001400IO08", F.lit("AXA coupon Ambition Monde 2"))
                              .when(F.col("CODE_ISIN") == "FR2CIBFS6232", F.lit("AXA Coupon 2024-2032"))
                              .otherwise("LES PROBLEMES"))
                            
                            .withColumn("Année",F.lit(int(annee)))  
                            .withColumn("Mois",F.lit(int(mois)))
                            .groupBy(F.col("Année"),F.col("mois"),F.col("Réseau"),F.col("FOND DE CAMPAGNE"))
                            .agg( F.sum(F.col("Collecte_Brute_TOTALE")).alias("Collecte_Brute"))     
                            .orderBy("Réseau")
                          
                          
                          )
    
    dataframes.append(Base_data_fds_campagne)

    return dataframes

# COMMAND ----------

for a in range(2022, 2025):
    for m in range(1, 13):
        if a == 2024 and m > 6:
            break

        # Formatage des variables mois et annee eb caractere
        mois = f"{m:02d}"  # sur 2 chiffres : "01"
        annee = str(a)

        dataframes = recup_AWS_RDU_et_initiative_UC(mois,annee)

# COMMAND ----------

dataframes

# COMMAND ----------

df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# COMMAND ----------

# TEST sur 1 mois

# df = recup_AWS("02","2023")

# COMMAND ----------

from axapy.functions import telecharger_dataframe

# COMMAND ----------

telecharger_dataframe(df_final)

# COMMAND ----------


