# Databricks notebook source

from pyspark.sql import DataFrame

def process_dataframe(df):


    #### On distingue 2 valeurs de réseau qui sont des anomalies
    #### 0   => toutes les lignes sont à 0 =>  à supprimer 
    #### #NA => représente qu'un seul contrat => aprsè recherche sur vivastation c'est partenaire bancaire
    #### 

    # supprimer les numero_contrat à 0  

    df = df.filter(col('NUMERO_CONTRAT')!=0)

    # supprimer les SITUATION_CONTRAT à 0  

    df = df.filter(col('SITUATION_CONTRAT')!="0")

    # supprimer les doublons lignes 

    df = df.dropDuplicates()

    # numero_contrat qui commence par "999" et qui a 7 chiffres au total : règle validée par François LANG
    # => code_portefeuille entre 9990000 et 9999999

    df = df.withColumn("INTERNATIONAL",(when(col("CODE_PORTEFEUILLE").between(9990000, 9999999),lit(1)).otherwise(lit(0)) )) 

    # Formatez contrat ( sur 16 )
    df= (df.withColumnRenamed("NUMERO_CONTRAT","numero_contrat1") 
        .withColumn("NUMERO_CONTRAT",lpad(col("numero_contrat1"), 16, '0')) 
        .drop("numero_contrat1") )

    # Formatez code portefeuille ( sur 10 )

    df= (df.withColumnRenamed("CODE_PORTEFEUILLE","CODE_PORTEFEUILLE1") 
        .withColumn("CODE_PORTEFEUILLE",lpad(col("CODE_PORTEFEUILLE1"), 10, '0')) 
        .drop("CODE_PORTEFEUILLE1") )

    # Renommez les réseaux

    AWE =df.withColumn("RESEAU",(when(col("RESEAU").contains("AEP"),lit("AEP"))
                                .when(col("RESEAU").contains("AGA"),lit("AGA"))
                                .when(col("RESEAU").contains("Agents LU"),lit("AGA"))
                                .when(col("RESEAU").contains("A2P"),lit("A2P"))
                            .when(col("RESEAU").contains("CGP"),lit("CGPI"))
                            .when(col("RESEAU").contains("Direct"),lit("GESTION PRIVEE DIRECTE"))
                            # on rajoute l'exception 
                            .when(col("RESEAU").contains("#NA"),lit("PARTENAIRE BANCAIRE"))
                            .when(col("RESEAU").like("%Part%"),lit("PARTENAIRE BANCAIRE"))
                            .otherwise(lit("AGA")) )) 

    # On créé la constante SYSTEME

    AWE =AWE.withColumn("SYSTEME",lit("AWE"))

    # Pour les variables quantitatives, on remplace la virgule par point et on converti

    ## MONTANT_BRUT
    AWE = AWE.withColumn("MONTANT_BRUT", trim(regexp_replace(col("MONTANT_BRUT")," ","")))
    AWE = AWE.withColumn("MONTANT_BRUT", trim(regexp_replace(col("MONTANT_BRUT"),",",".")).cast("double"))

    ## MONTANT_NET
    AWE = AWE.withColumn("MONTANT_NET", trim(regexp_replace(col("MONTANT_NET")," ","")))
    AWE = AWE.withColumn("MONTANT_NET", trim(regexp_replace(col("MONTANT_NET"),",",".")).cast("double"))

    ## PM_FIN
    AWE = AWE.withColumn("PM_FIN", trim(regexp_replace(col("PM_FIN")," ","")))
    AWE = AWE.withColumn("PM_FIN", trim(regexp_replace(col("PM_FIN"),",",".")).cast("double"))
                    

    # display(Base_AWE)

    return AWE


# COMMAND ----------


