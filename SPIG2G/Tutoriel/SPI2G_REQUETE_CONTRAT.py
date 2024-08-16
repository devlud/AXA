# Databricks notebook source
print("=======================================================================================================================================")
print("")
print("Auteur du Programme : AGKAYA IBRAHIM")
print("Date de création : 08/11/2023")
print("") 
print("=======================================================================================================================================")

# COMMAND ----------

date_comptable='20231231'
numero_contrat=['0000001051597317']

# COMMAND ----------


#Chargement de la table existante Rxd Générique
#chargement des tables assurances faits et RxD Class
from axapy.labs.piper import Spig2sReader
import axapy.functions as func
import pyspark.sql.functions as F
Debug = False
Spig2sRead = Spig2sReader()

# COMMAND ----------

Spig2sRead.reset_criteria('all')

# COMMAND ----------

Spig2sRead.set_calc_annemois('Cumul N',date_comptable)

Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['CTA_NUMERO']) #Numero contrat

Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM']) 

Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_6') 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['Réseau Axa France']) 


Spig2sRead.set_criteria('marche','colonne_filtre','L4_OBJ_LBR') 
Spig2sRead.set_criteria('marche','valeurs_filtre',['Total Marché AXA France']) 

Spig2sRead.set_criteria('uj','colonne_filtre','L6_UJ_LBR') 
Spig2sRead.set_criteria('uj','valeurs_filtre',['Total SA + Mutuelles'])

Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle']) 


Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L4_LBR_DESC","L6_LBR_DESC","L7_LBR_DESC","L9_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L7_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Emissions Totales','Prestations y compris acceptations' ])

# COMMAND ----------

Spig2sRead.actual_criteria()

# COMMAND ----------

df_result = Spig2sRead.execute()
df_result = (df_result
.filter(F.col("indicateurs_L9_LBR_DESC").isin(["> Autres indicateurs (Montant)"]))
.drop("indicateurs_L9_LBR_DESC")
.filter(F.col('maille_fine_CTA_NUMERO').isin(numero_contrat)))
df_result.display()

# COMMAND ----------

#Spig2sRead.help()

# COMMAND ----------

#Spig2sRead.actual_criteria()
