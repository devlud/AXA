# Databricks notebook source
date_comptable='20231231'

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
#Spig2sRead.set_criteria('annemois','valeurs_filtre',['20231201']) 

#Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['CTA_NUMERO']) #Numero contrat

Spig2sRead.set_criteria('maille_fine','colonnes_affichage',['CTA_NUMERO']) #Numero contrat
Spig2sRead.set_criteria('maille_fine','colonne_filtre','CTA_NUMERO') 
Spig2sRead.set_criteria('maille_fine','valeurs_filtre',['0000001050623417']) 

Spig2sRead.set_criteria('source','colonne_filtre','L6_OBJ_COD') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('source','valeurs_filtre',['VISSOC_NSM']) 

Spig2sRead.set_criteria('reseau','colonnes_affichage',['Res_Com_Niv_4']) 
Spig2sRead.set_criteria('reseau','colonne_filtre','Res_Com_Niv_4') #Filtre standard du Cube sur 'Vision sociale (Neuflize à  40%) 
Spig2sRead.set_criteria('reseau','valeurs_filtre',['A2P']) 


Spig2sRead.set_criteria('marche','colonne_filtre','L4_OBJ_LBR') #Filtre standard du Cube sur 'Total Marché AXA France 
Spig2sRead.set_criteria('marche','valeurs_filtre',['Total Marché AXA France']) 

Spig2sRead.set_criteria('uj','colonne_filtre','L3_UJ_LBR') #Filtre standard du Cube sur 'Total UJ AXA France 
Spig2sRead.set_criteria('uj','valeurs_filtre',['AXA France VIE SA']) ###

Spig2sRead.set_criteria('produits','colonne_filtre','L7_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['Total Epargne Individuelle']) 
#Spig2sRead.set_criteria('produits','colonnes_affichage',['L3_SUPPORT_LBR']) 


#Spig2sRead.set_criteria('produits','colonne_filtre','L4_PILOTECO_AGA_LBR') ### filtre les produits retraites 
#Spig2sRead.set_criteria('produits','valeurs_filtre',['Retraite aidée sous objectif']) 

Spig2sRead.set_criteria('produits','colonne_filtre','L3_PRODUIT_LBR') 
Spig2sRead.set_criteria('produits','valeurs_filtre',['FAR PER']) 

Spig2sRead.set_criteria('indicateurs','colonnes_affichage',["L2_LBR_DESC","L4_LBR_DESC","L6_LBR_DESC","L9_LBR_DESC"]) 
Spig2sRead.set_criteria('indicateurs','colonne_filtre','L6_LBR_DESC') 
Spig2sRead.set_criteria('indicateurs','valeurs_filtre',['Emissions Totales',
                                                        "Emissions Vie VC",
                                                        "Emissions Vie PUVL OD",
                                                        "Emissions Vie PU","Emissions Vie 1er VL",
                                                        "Emissions PP (hors OD)",
                                                        "Emissions Vie VC sur PP",
                                                        "Emissions Vie PUVL TOTAL yc Acceptations",
                                                        "Emissions PP OD (yc annulations)",
                                                        "Affaires nouvelles nettes de sans effet"                     
                                                        ])

# COMMAND ----------

Spig2sRead.actual_criteria()

# COMMAND ----------

df_result = Spig2sRead.execute()
df_result = (df_result
.filter(F.col("indicateurs_L9_LBR_DESC").isin(["> Indicateurs Contrats (Montant)","> Indicateurs Production (Montant)"])))
df_result.display()

# COMMAND ----------


