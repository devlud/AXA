# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CRITERES
# MAGIC
# MAGIC - Total Epargne Individuelle :	[04 Activite].[041 Produits].[All].[Total Vie Individuelle].[Total Epargne Individuelle] : OK
# MAGIC - Support Epargne :             [04 Activite].[042 Supports].[All].[Support Epargne] : OK
# MAGIC
# MAGIC - Réseau Axa France :           [05 Distribution].[052 Reseau Commercial].[All].[Réseau Axa France] : OK
# MAGIC - Vision sociale hors AWE : [14 Source].[Source].[All].[Vision sociale hors AWE] : OK
# MAGIC - Total UJ AXA France : [08 UJ].[UJ].[All].[Total UJ AXA France] : OK
# MAGIC
# MAGIC # INDICATEURS : Rachat partiel et Rachat total
# MAGIC
# MAGIC - Rachat partiel : [03 Indicateurs].[Indicateurs].[All].[> Indicateurs en montant].[> Indicateurs Sinistres (Montant)].[Prestations y compris acceptations].[Prestations hors acceptations].[Rachats].[Rachat partiel]
# MAGIC - Rachat total : [03 Indicateurs].[Indicateurs].[All].[> Indicateurs en montant].[> Indicateurs Sinistres (Montant)].[Prestations y compris acceptations].[Prestations hors acceptations].[Rachats].[Rachat total]
# MAGIC - Nombre de rachats partiels : [03 Indicateurs].[Indicateurs].[All].[> Indicateurs en nombre].[> Prestations Vie (nombre)].[Nombre de prestations Vie].[Nombre de rachats (totaux / partiels / programmés)].[Nombre de rachats partiels]
# MAGIC - Nombre de rachats totaux : [03 Indicateurs].[Indicateurs].[All].[> Indicateurs en nombre].[> Prestations Vie (nombre)].[Nombre de prestations Vie].[Nombre de rachats (totaux / partiels / programmés)].[Nombre de rachats totaux]

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

Spig2sRead = Spig2sReader()

# COMMAND ----------


# Supprime tous les critères 

Spig2sRead.reset_criteria('all')

# COMMAND ----------

# critères actuels

Spig2sRead.actual_criteria()

# COMMAND ----------

annee="2024"
mois="07"

# COMMAND ----------

# MAGIC %md
# MAGIC ## VISION 

# COMMAND ----------

Spig2sRead.get_criteria('vision').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MAILLE FINE & ANNEEMOIS 

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



Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['DATE_COMPTABLE','DATE_EFFET',"func_month"]) #"DATE_EFFET","tech_filename","func_month"
Spig2sRead.set_criteria('annemois','valeurs_filtre',date) 

# COMMAND ----------

#Spig2sRead.set_calc_annemois('Cumul N',date_comptable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SOURCE

# COMMAND ----------

#Spig2sRead.get_criteria('source').display()

# COMMAND ----------

df = Spig2sRead.get_criteria('source')

#df.select(F.col("L6_OBJ_COD")).distinct().display()

df.filter(F.col("L6_OBJ_COD").like("%VISSOC_NSM%")).distinct().display()

# COMMAND ----------


###
   # Pour filtrer AWE, dans le cube, on prend comme L7 = "Vision sociale hors AWE" 
   # Quand on applique ce filtre dans spig2S, ça ne marche pas => ça sort rien => pk ?
   # Donc on va dans un premier temps, sélectionner (yc AWE) avec L6 = "VISSOC_NSM"
   # et ensuite avec L2(=ALIMMAN_AXALUX) en affichage, on retire AWE qu'on sait isoler, qu'on peut faire sur excel pour avoir les deux visions !!


# SOURCE

Spig2sRead.set_criteria('source','colonnes_affichage',['L2_OBJ_COD'])
Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM']) #### (seulement luxembourg <=> L2=ALIMMAN_AXALUX)    et    VISSOC_NSM  ==yc AWE

# COMMAND ----------

# MAGIC %md
# MAGIC ## RESEAU & MARCHE

# COMMAND ----------

Spig2sRead.get_criteria('reseau').display()

# COMMAND ----------

# SOURCE

# CUBE Axa France : MEMBRECUBE("Cube CdG";"[05 Distribution].[052 Reseau Commercial].[All].[Réseau Axa France]")

Spig2sRead.set_criteria('reseau','colonnes_affichage',['Res_Com_Niv_4','Res_Com_Niv_3','Region_Com_Niv_2_Detail_Cube']) 
Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_6') 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['Réseau Axa France'])


# MARCHE

Spig2sRead.set_criteria('marche','colonne_filtre','L4_OBJ_LBR') #Filtre standard du Cube sur 'Total Marché AXA France 
Spig2sRead.set_criteria('marche','valeurs_filtre',['Total Marché AXA France']) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## UJ

# COMMAND ----------

Spig2sRead.get_criteria('uj').display()

# COMMAND ----------


# CUBE : MEMBRECUBE("Cube CdG";"[08 UJ].[UJ].[All].[Total UJ AXA France]")

# UJ : mutuelle ou SA

Spig2sRead.set_criteria('uj','colonnes_affichage',["L6_UJ_PRISM_LBR"]) 
Spig2sRead.set_criteria('uj','colonne_filtre','L6_UJ_PRISM_LBR') #Filtre standard du Cube sur 'Total UJ AXA France 
Spig2sRead.set_criteria('uj','valeurs_filtre',['Total UJ AXA France']) ###

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRODUITS

# COMMAND ----------

Spig2sRead.get_criteria('produits').display()

# COMMAND ----------

# PRODUIT EPARGNE INDIVIDUEL // Support Epargne

# CUBE : [04 Activite].[041 Produits].[All].[Total Vie Individuelle].[Total Epargne Individuelle] = L7_PRODUIT_LBR
# CUBE : [04 Activite].[042 Supports].[All].[Support Epargne] = L5_SUPPORT_LBR

Spig2sRead.set_criteria('produits','colonnes_affichage',["L5_SUPPORT_LBR"]) 
Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle']) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## INDICATEURS

# COMMAND ----------

Spig2sRead.get_criteria('indicateurs').display()

# COMMAND ----------

#df = Spig2sRead.get_criteria('indicateurs')
#df.filter(F.col("L9_LBR_DESC").like("%> Prestations Vie (nombre)%")).distinct().display()

# COMMAND ----------

df = Spig2sRead.get_criteria('indicateurs')
df = df.filter(F.upper(F.col("L5_LBR_DESC")).like("%RACHAT%"))
df.select(F.col("L5_LBR_DESC")).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Montant Rachat

# COMMAND ----------


######
      # INDICATEURS
######


#################MONTANTS

## CUBE ISIS pour les MONTANTS des rachats partiels 

#"[03 Indicateurs].[Indicateurs].[All].[> Indicateurs en montant].[> Indicateurs Sinistres (Montant)].[Prestations y compris acceptations].[Prestations hors # acceptations].[Rachats].[Rachat partiel]]"

# On voit qu'à partir de [All] (sans compter [All]) on n'a 6 blocs :

# 1er bloc = [> Indicateurs en montant]
# 2ème bloc = [> Indicateurs Sinistres (Montant)]
# 3ème bloc = [Prestations y compris acceptations]
# 4ème bloc = [Prestations hors # acceptations]
# 5ème bloc = [Rachats]
# 6ème bloc = [Rachat partiel]

######
      #  Donc pour avoir une TABLE DE CORRESPONDANCE, on va compter le nombre de colonnes à partir de la colonne la plus agrégeée :
######

## le plus agrégées = "L10_LBR_DESC" correpond à [> Indicateurs en montant], ensuite on n'a le code "L10_LBR_DESC" qui correspond à la même variable donc on compte pas
## Ensuite, "L9_LBR_DESC" correpond à [> Indicateurs Sinistres (Montant)], ensuite on n'a le code "L9_LBR_DESC" qui correspond à la même variable donc on compte pas
## Ensuite, "L8_LBR_DESC" correpond à [Prestations y compris acceptations], ensuite on n'a le code "L8_LBR_DESC" qui correspond à la même variable donc on compte pas
## Ensuite, "L7_LBR_DESC" correpond à [Prestations hors # acceptations], ensuite on n'a le code "L7_LBR_DESC" qui correspond à la même variable donc on compte pas
## Ensuite, "L6_LBR_DESC" correpond à [Rachats], ensuite on n'a le code "L6_LBR_DESC" qui correspond à la même variable donc on compte pas
## Ensuite, "L5_LBR_DESC" correpond à [Rachat partiel], ensuite on n'a le code "L5_LBR_DESC" qui correspond à la même variable donc on compte pas

###### => Donc c'est L5 qu'on choisira pour sélectionner [Rachat partiel] tout simplement !!


###### /!\ néanmoins, quand on regarde le NOMBRE de rachats partiels, dasn la cube c'est :

# [03 Indicateurs].[Indicateurs].[All].[> Indicateurs en nombre].[> Prestations Vie (nombre)].[Nombre de prestations Vie].[Nombre de rachats (totaux / partiels / programmés)].[Nombre de rachats partiels]]

# après [All], on n'a 5 blocs et pas 6 blocs contrairement au montant donc on a 1 bloc en moins et donc la colonne sera "L6_LBR_DESC"

##### => On sera donc obligé de créer 2 TABLES DIFFERENTES CAR L'une pointe vers L5 et l'autre vers L6 mas dans SPIG2S, on peut filtrer que sur une seule colonne !!


##### On fait les montants 

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L5_LBR_DESC","L8_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L5_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Rachat total','Rachats OD','Rachats acceptations','Rachat partiel','Rachat fourgous','Rachat programmé'])

# COMMAND ----------

Spig2sRead.actual_criteria()

# COMMAND ----------

# MAGIC %md
# MAGIC ## On EXECUTE AVEC LES FILTRES UTILISES

# COMMAND ----------

# EXECUTION DE LA REQUETE AVEC LES FILTRES SELECTIONNES EN AMONT

df_result = Spig2sRead.execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MISE AU PROPRE

# COMMAND ----------

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

# COMMAND ----------

df_result.printSchema()

# COMMAND ----------


# On prend les variables que l'on a choisit en amont en rajouter la dimension devant
# par exemple, on a choisit la variable "L5_SUPPORT_LBR" de la dimension "produits" donc mettre "produits_L5_SUPPORT_LBR"

Base_MONTANT_rachat=(df_result.filter(F.col("produits_L5_SUPPORT_LBR")=="Support Epargne")
           .groupBy(F.col("maille_fine_DATE_COMPTABLE"),F.col("maille_fine_DATE_EFFET"),F.col("maille_fine_func_month"),F.col("indicateurs_L5_LBR_DESC"),F.col("indicateurs_L8_LBR_DESC"),F.col("source_L2_OBJ_COD"),F.col("RESEAU"))
          .agg(   F.sum(F.col("Value")).alias("MONTANT"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Nombre Rachat

# COMMAND ----------

## On modifie seulement indicateurs

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L6_LBR_DESC","L8_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L6_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Nombre de rachats totaux','Nombre de rachats partiels'])

# EXECUTION

df_result = Spig2sRead.execute()

# COMMAND ----------

Base_NOMBRE_rachat=(df_result.filter(F.col("produits_L5_SUPPORT_LBR")=="Support Epargne")
           .groupBy(F.col("maille_fine_DATE_COMPTABLE"),F.col("maille_fine_DATE_EFFET"),F.col("maille_fine_func_month"),F.col("indicateurs_L6_LBR_DESC"),F.col("indicateurs_L8_LBR_DESC"),F.col("source_L2_OBJ_COD"))
          .agg(   F.sum(F.col("Value")).alias("MONTANT"))
)

# COMMAND ----------

Base_RACHAT = Base_MONTANT_rachat.unionByName(Base_NOMBRE_rachat,allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TELECHARGEMENT

# COMMAND ----------

telecharger_dataframe(Base_RACHAT)

# COMMAND ----------


