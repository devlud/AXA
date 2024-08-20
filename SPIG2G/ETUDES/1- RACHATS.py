# Databricks notebook source
from axapy.labs.piper import Spig2sReader
import axapy.functions as func
import pyspark.sql.functions as F
Debug = False
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
from pyspark.sql.functions import when,substring, month, year, lit, desc, concat_ws, dense_rank, row_number, lag, max, lead, to_date, date_format, concat_ws,lpad
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from pyspark.sql.window import Window

Spig2sRead = Spig2sReader()

# COMMAND ----------

annee="2024"
mois="07"

# COMMAND ----------

# Génération d'une liste sur 2 ans

annee_N=int(annee)
annee_N_1=annee_N-2
Mois_N=int(mois)+1

from datetime import datetime

def generate_filtered_month_start_dates(start_year, end_year):
    date_list = []
    
# Itérer sur chaque année spécifiée

    for year in range(start_year, end_year + 1):
        for month in range(1, 13): 
            date_str = f"{year:04d}{month:02d}01"
            date_list.append(date_str)
    
    return date_list

# Exemple d'utilisation
start_year = annee_N_1
end_year = annee_N
date= generate_filtered_month_start_dates(start_year, end_year)

# COMMAND ----------

# MAGIC %md
# MAGIC # MONTANT RACHAT

# COMMAND ----------


#MOIS 

Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['DATE_COMPTABLE','DATE_EFFET',"func_month"]) #"DATE_EFFET","tech_filename","func_month"
Spig2sRead.set_criteria('annemois','valeurs_filtre',date) 

# SOURCE

Spig2sRead.set_criteria('source','colonnes_affichage',['L2_OBJ_COD'])
Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD')
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM'])

# RESEAU

Spig2sRead.set_criteria('reseau','colonnes_affichage',['Res_Com_Niv_4','Res_Com_Niv_3','Region_Com_Niv_2_Detail_Cube']) 
Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_6') 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['Réseau Axa France'])

# MARCHE

Spig2sRead.set_criteria('marche','colonne_filtre','L4_OBJ_LBR') #Filtre standard du Cube sur 'Total Marché AXA France 
Spig2sRead.set_criteria('marche','valeurs_filtre',['Total Marché AXA France']) 

# UJ

Spig2sRead.set_criteria('uj','colonnes_affichage',["L6_UJ_PRISM_LBR"]) 
Spig2sRead.set_criteria('uj','colonne_filtre','L6_UJ_PRISM_LBR') #Filtre standard du Cube sur 'Total UJ AXA France 
Spig2sRead.set_criteria('uj','valeurs_filtre',['Total UJ AXA France'])

# PRODUIT 

Spig2sRead.set_criteria('produits','colonnes_affichage',["L5_SUPPORT_LBR"]) 
Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle']) 

# INDICATEUR

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L5_LBR_DESC","L8_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L5_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Rachat total','Rachats OD','Rachats acceptations','Rachat partiel','Rachat fourgous','Rachat programmé'])

# EXECUTION

df_result = Spig2sRead.execute()


# COMMAND ----------


# Création du Réseau

df_result=(df_result.withColumn(
        "RESEAU",
        F.when(F.col("reseau_Res_Com_Niv_4").isin(["A2P"]), F.lit("A2P"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["AG (AGGX)"]), F.lit("AGA"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["Autres Salariés", "Salariés Classiques"]), F.lit("AEP"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["DOM-TOM"]), F.lit("DOM-TOM"))
        .when(F.col("reseau_Res_Com_Niv_3").isin(["Partenariats bancaires hors Neuflize et Natio"]), F.lit("Partenariats Bancaires"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["Partenariats Distribution"]), F.lit("Partenariats Distribution"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["CGP"]), F.lit("CGP"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["Courtiers généralistes (hors MGARD)"]), F.lit("Courtiers"))
        .when(F.col("reseau_Res_Com_Niv_3").isin(["Gestion Privée Directe"]), F.lit("Gestion Privée Directe"))
        .when(F.col("reseau_Res_Com_Niv_4").isin(["DOM-TOM"]), F.lit("DOM-TOM"))
        .when(F.col("reseau_Region_Com_Niv_2_Detail_Cube").isin(["AXA Partenaires"]), F.lit("Axa Partenaires"))
        .when(F.col("reseau_Res_Com_Niv_3").isin(["Partenariat bancaire Neuflize"]), F.lit("Neuflize"))
        .otherwise("Autres")
    )
)

# Calcul de la valeur

Base_MONTANT_rachat=(df_result.filter(F.col("produits_L5_SUPPORT_LBR")=="Support Epargne")
           .groupBy(F.col("maille_fine_DATE_COMPTABLE"),F.col("maille_fine_DATE_EFFET"),F.col("maille_fine_func_month"),F.col("indicateurs_L5_LBR_DESC"),F.col("indicateurs_L8_LBR_DESC"),F.col("source_L2_OBJ_COD"),F.col("RESEAU"))
          .agg(   F.sum(F.col("Value")).alias("MONTANT"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # NOMBRE RACHATS

# COMMAND ----------

## Nombre rachats

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L6_LBR_DESC","L8_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L6_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Nombre de rachats totaux','Nombre de rachats partiels'])

# EXECUTION

df_result = Spig2sRead.execute()

# COMMAND ----------

# Calcul du nombre


Base_NOMBRE_rachat=(df_result.filter(F.col("produits_L5_SUPPORT_LBR")=="Support Epargne")
           .groupBy(F.col("maille_fine_DATE_COMPTABLE"),F.col("maille_fine_DATE_EFFET"),F.col("maille_fine_func_month"),F.col("indicateurs_L6_LBR_DESC"),F.col("indicateurs_L8_LBR_DESC"),F.col("source_L2_OBJ_COD"))
          .agg(   F.sum(F.col("Value")).alias("MONTANT"))
)

# COMMAND ----------

# aggrégation verticale

Base_RACHAT = Base_MONTANT_rachat.unionByName(Base_NOMBRE_rachat,allowMissingColumns=True)

# COMMAND ----------

# Spécificités : raisonnement en semaine du trimestre

#  Mois

Base_RACHAT = Base_RACHAT.withColumn("mois", F.month(F.col("maille_fine_DATE_EFFET")))

#  Année

Base_RACHAT = Base_RACHAT.withColumn("annee", F.year(F.col("maille_fine_DATE_EFFET")))

#  Calcul de la semaine du Trimestre 

                            #  Calcul du trimestre 
Base_RACHAT = ( Base_RACHAT.withColumn("trimestre", F.quarter(F.col("maille_fine_DATE_EFFET")))
                            #  Calcul de la semaine de l'année
                           .withColumn("semaine_annee", F.weekofyear(F.col("maille_fine_DATE_EFFET")))
                            #  Calcul de la première semaine du trimestre
                           .withColumn("premiere_semaine_trimestre",F.weekofyear(F.expr("date_sub(trunc(maille_fine_DATE_EFFET,'quarter'),-1)")))
                            #  Calcul de la semaine dans le trimestre
                           .withColumn("semaine_trimestre",F.expr("semaine_annee - premiere_semaine_trimestre + 1"))
                           )


Base_RACHAT.select(F.col("maille_fine_DATE_EFFET"),F.col("trimestre"),F.col("semaine_annee"),F.col("premiere_semaine_trimestre"),F.col("semaine_trimestre")).display()

# COMMAND ----------

telecharger_dataframe(Base_RACHAT)
