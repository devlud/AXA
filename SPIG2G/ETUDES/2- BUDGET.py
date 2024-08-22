# Databricks notebook source
annee="2019"
mois="12"

# COMMAND ----------

from axapy.labs.piper import Spig2sReader
import axapy.functions as func
import pyspark.sql.functions as F
Debug = False
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
from pyspark.sql.functions import when,substring, month, year, lit, desc, concat_ws, dense_rank, row_number, lag, max, lead, to_date, date_format, concat_ws,lpad
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, ArrayType, DecimalType, DateType
from pyspark.sql import DataFrame, SQLContext, Row
from pyspark.sql.window import Window

# COMMAND ----------

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


Spig2sRead = Spig2sReader()

# COMMAND ----------


Spig2sRead.reset_criteria('all')
Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['DATE_COMPTABLE','CTA_NUMERO',"tech_filename","func_month"]) #"DATE_EFFET","tech_filename","func_month"
Spig2sRead.set_criteria('annemois','valeurs_filtre',date) 

Spig2sRead.set_criteria('source','colonnes_affichage',["L4_OBJ_LBR","L2_OBJ_LBR"]) 
Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM']) #### SOCIAL_HORS_LUX       VISSOC_NSM  ==yc AWE

Spig2sRead.set_criteria('reseau','colonnes_affichage',["Res_Com_Niv_3",'Res_Com_Niv_4',"Res_Com_Niv_6","Res_Com_Niv_2_Detail_Cube","Region_Com_Niv_2_Detail_Cube"]) 
Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_6') 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['Réseau Axa France']) 

Spig2sRead.set_criteria('marche','colonne_filtre','L4_OBJ_LBR') #Filtre standard du Cube sur 'Total Marché AXA France 
Spig2sRead.set_criteria('marche','valeurs_filtre',['Total Marché AXA France']) 

Spig2sRead.set_criteria('pyramide','colonnes_affichage',["port_cod","Portefeuille","PP_1_Delegation","PP_2_Circonsription"]) 

Spig2sRead.set_criteria('region','colonnes_affichage',["OBJ_LBR"]) 

Spig2sRead.set_criteria('uj','colonnes_affichage',["L3_UJ_PRISM_LBR"]) 
Spig2sRead.set_criteria('uj','colonne_filtre','L6_UJ_PRISM_LBR') #Filtre standard du Cube sur 'Total UJ AXA France 
Spig2sRead.set_criteria('uj','valeurs_filtre',['Total UJ AXA France']) ###

Spig2sRead.set_criteria('produits','colonnes_affichage',['L5_PRISM_LBR','L2_PRODUIT_LBR','L3_SUPPORT_LBR','L4_SUPPORT_LBR','L7_PRODUIT_LBR','L4_PILOTECO_AGA_LBR'])
Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle','Total Prévoyance']) 

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L2_LBR_DESC","L4_LBR_DESC","L6_LBR_DESC","L7_LBR_DESC","L9_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L7_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',["Emissions PP (yc OD)","Emissions Vie PUVL Total","Prestations y compris acceptations","Emissions Totales","Apport Net"])

# COMMAND ----------

Base_1 = Spig2sRead.execute()

# COMMAND ----------

 Spig2sRead.reset_criteria('all')
Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['DATE_COMPTABLE','CTA_NUMERO',"tech_filename","func_month"]) #"DATE_EFFET","tech_filename","func_month"
Spig2sRead.set_criteria('annemois','valeurs_filtre',date) 

Spig2sRead.set_criteria('source','colonnes_affichage',["L4_OBJ_LBR","L2_OBJ_LBR"]) 
Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM']) #### SOCIAL_HORS_LUX       VISSOC_NSM  ==yc AWE

Spig2sRead.set_criteria('reseau','colonnes_affichage',["Res_Com_Niv_3",'Res_Com_Niv_4',"Res_Com_Niv_6","Res_Com_Niv_2_Detail_Cube","Region_Com_Niv_2_Detail_Cube"]) 
Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_6') 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['Réseau Axa France']) 

Spig2sRead.set_criteria('pyramide','colonnes_affichage',["port_cod","Portefeuille","PP_1_Delegation"]) 

Spig2sRead.set_criteria('region','colonnes_affichage',["OBJ_LBR"]) 

Spig2sRead.set_criteria('uj','colonnes_affichage',["L3_UJ_PRISM_LBR"]) 
Spig2sRead.set_criteria('uj','colonne_filtre','L6_UJ_PRISM_LBR') #Filtre standard du Cube sur 'Total UJ AXA France 
Spig2sRead.set_criteria('uj','valeurs_filtre',['Total UJ AXA France']) ###

Spig2sRead.set_criteria('produits','colonnes_affichage',['L5_PRISM_LBR','L2_PRODUIT_LBR','L3_SUPPORT_LBR','L4_SUPPORT_LBR','L7_PRODUIT_LBR','L4_PILOTECO_AGA_LBR'])
Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle','Total Prévoyance']) 

Spig2sRead.set_calc_indicateur('APE') 


# COMMAND ----------

Base_2 = Spig2sRead.execute()

Base_2 = (Base_2.withColumn("indicateurs_L7_LBR_DESC",F.when( F.col("indicateur_APE").isin(["APE EPARGNE - REGULAR","APE EPARGNE - SINGLE AN","APE EPARGNE - SINGLE VC","APE PREVOYANCE"]) ,F.lit("Ape"))
                            .otherwise(" ")).drop("indicateur_APE"))

# COMMAND ----------

Base_SPI2G = (
    Base_1.unionByName(Base_2, allowMissingColumns=True)
    .withColumn("maille_fine_func_month", to_date(F.col("maille_fine_func_month")))
    .withColumn("MOIS", date_format(F.col("maille_fine_func_month"), "MM"))
    .withColumn("ANNEE", date_format(F.col("maille_fine_func_month"), "yyyy"))
   # .filter(~((F.col("ANNEE") == annee) & (F.col("MOIS") > mois)))
    .withColumn(
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
    .withColumn(
        "REGION",
        F.when((F.col("region_OBJ_LBR").isin(["Ile de France"])) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("ILE DE France"))
        .when((F.col("region_OBJ_LBR").isin(["Nord-est"])) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("NORD EST"))
        .when((F.col("region_OBJ_LBR").isin(["Ouest"])) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("OUEST"))
        .when((F.col("region_OBJ_LBR").isin(["Sud-est"])) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("SUD EST"))
        .when((F.col("region_OBJ_LBR").isin(["Sud-ouest"])) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("SUD OUEST"))
        .when((~(F.col("region_OBJ_LBR").isin(["Sud-ouest", "Sud-est", "Ouest", "Nord-est", "Ile de France"]))) & (F.col("RESEAU").isin(["Courtiers", "AGA", "AEP"])), F.lit("Divers"))
        .when((F.col("RESEAU").isin(["CGP"])), F.lit("AXA WEALTH MANAGEMENT"))
        .when((F.col("RESEAU").isin(["Gestion Privée Directe"])), F.lit("AXA WEALTH MANAGEMENT"))
        .when((F.col("RESEAU").isin(["Partenariats Distribution"])), F.lit("AXA WEALTH MANAGEMENT"))
        .when((F.col("RESEAU").isin(["Partenariats Bancaires"])), F.lit("AXA WEALTH MANAGEMENT"))
        .when((F.col("RESEAU").isin(["A2P"])), F.lit("A2P"))
        .when((F.col("RESEAU").isin(["Neuflize"])), F.lit("AUTRES"))
        .when((F.col("RESEAU").isin(["DOM-TOM"])), F.lit("AUTRES"))
        .when((F.col("RESEAU").isin(["Axa Partenaires"])), F.lit("AUTRES"))
        .otherwise(" ")
    )
    .withColumn(
        "TOP_EPAR_RET_PREV",
        F.when(F.col("produits_L4_PILOTECO_AGA_LBR").isin(["Retraite aidée sous objectif"]), F.lit("RETRAITE"))
        .when(F.col("produits_L7_PRODUIT_LBR").isin(["Total Prévoyance"]), F.lit("PREVOYANCE"))
        .otherwise("EPARGNE")
    )
    .filter(F.col("TOP_EPAR_RET_PREV").isin(["RETRAITE", "EPARGNE"]))
    .withColumn(
        "UJ",
        F.when(F.col("uj_L3_UJ_PRISM_LBR").isin(["AXA France VIE SA"]), F.lit("SA yc FPRS"))
        .when(F.col("uj_L3_UJ_PRISM_LBR").isin(["Autres SA"]), F.lit("AUTRES SA"))
        .otherwise("MU")
    )
    .withColumn(
        "produits_L4_SUPPORT_LBR",
        F.when(F.col("produits_L3_SUPPORT_LBR").isin(["EUROCROISSANCE"]), F.lit("Support EUROCROISSANCE"))
        .otherwise(F.col("produits_L4_SUPPORT_LBR"))
    )
    .withColumn(
        "SUPPORT",
        F.when(F.col("produits_L4_SUPPORT_LBR").isin(["Support UC"]), F.lit("UC"))
        .when(F.col("produits_L4_SUPPORT_LBR").isin(["Support EURO"]), F.lit("EURO"))
        .otherwise(F.lit("EUROCROISSANCE"))
    )
    .withColumn(
        "VISION_LUX",
        F.when(F.col("source_L2_OBJ_LBR").isin(["Saisie manuelle AXALUX"]), F.lit("OUI"))
        .otherwise(F.lit("NON"))
    )
    .withColumn(
        "SYSTEME",
        F.when(F.col("source_L4_OBJ_LBR").isin(["Alimentations manuelles"]), F.lit("Alimentations manuelles"))
        .otherwise(F.col("source_L2_OBJ_LBR"))
    )
    .withColumn(
        "MT_REEL_MOIS_N",
        F.when((F.col("ANNEE") == annee) & (F.col("MOIS") <= mois), F.col("Value")).otherwise(0)
    )
    .withColumn(
        "MT_REEL_MOIS_N_1",
        F.when((F.col("ANNEE") == str(int(annee) - 1)) & (F.col("MOIS") <= mois), F.col("Value")).otherwise(0)
    )
    .withColumn(
        "MT_REEL_ANNEE_N_1",
        F.when((F.col("ANNEE") == str(int(annee) - 1)), F.col("Value")).otherwise(0)
    )
    .withColumn(
        "MT_REEL_ANNEE_N_2",
        F.when((F.col("ANNEE") == str(int(annee) - 2)), F.col("Value")).otherwise(0)
    )
    .withColumn(
        "NATURE",
        F.when(
            (F.col('indicateurs_L4_LBR_DESC').isin(["Affaires nouvelles à PP non temporaire sans effet", "Affaires nouvelles à PP non temporaire"])) & 
            (F.col("indicateurs_L9_LBR_DESC") == "> Indicateurs Production (Montant)"),
            F.lit("Affaires nouvelles à PP non temporaire nettes de sans effet")
        )
        .when(
            (F.col('indicateurs_L4_LBR_DESC').isin(["Affaires nouvelles à VLP", "Affaires nouvelles à VLP sans effet"])) & 
            (F.col("indicateurs_L9_LBR_DESC") == "> Indicateurs Production (Montant)"),
            F.lit("Affaires nouvelles à VLP net de sans effet")
        )
        .when(
            (F.col('indicateurs_L7_LBR_DESC').isin(["Emissions Vie PUVL Total"])) & 
            (F.col("indicateurs_L9_LBR_DESC") == "> Indicateurs Contrats (Montant)"),
            F.lit("Emissions Vie PUVL Total")
        )
        .when(
            (F.col('indicateurs_L7_LBR_DESC').isin(["Emissions PP (yc OD)"])) & 
            (F.col("indicateurs_L9_LBR_DESC") == "> Indicateurs Contrats (Montant)"),
            F.lit("Emissions PP (yc OD)")
        )
        .when(
            (F.col('indicateurs_L7_LBR_DESC').isin(["Prestations y compris acceptations"])) & 
            (F.col("indicateurs_L9_LBR_DESC") == "> Autres indicateurs (Montant)"),
            F.lit("Prestations y compris acceptations")
        )
        .when(F.col('indicateurs_L7_LBR_DESC').isin(["Ape"]), F.lit("Ape"))
        .otherwise(F.lit("ND"))
    )
    .filter(~(F.col("NATURE") == "ND"))
    .withColumn(
        "NATURE2",
        F.when(F.col("NATURE").isin(["Emissions Vie PUVL Total", "Emissions PP (yc OD)"]), F.lit("Emissions Totales"))
        .otherwise(F.col("NATURE"))
    )
    .withColumn(
        "DETAILS_EMISSIONSVIE",
        F.when(F.col('indicateurs_L4_LBR_DESC').isin(["Emissions Vie VLP"]), F.lit("Emissions Vie VLP"))
        .otherwise(0)
    )
)

Base_SPI2G=(Base_SPI2G
           .groupBy(F.col("UJ"),F.col("TOP_EPAR_RET_PREV"),F.col("VISION_LUX"),F.col("SYSTEME"),F.col("MOIS"),F.col("ANNEE"),F.col("RESEAU"),F.col("REGION"),F.col("NATURE"),F.col("NATURE2"),F.col("DETAILS_EMISSIONSVIE"))
          .agg(   F.sum(F.col("Value")).alias("MONTANT")))

# COMMAND ----------

telecharger_dataframe(Base_SPI2G)

# COMMAND ----------

#df_produits = Spig2sRead.get_criteria('indicateurs').display()
