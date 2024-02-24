# Databricks notebook source
## On prend le mois et l'année du mois précédent

annee="2022" #Indiquer l'annee 
mois="12"   #Indiquer le mois 

# COMMAND ----------

# Refonte du code WPS : "Extraction_donnees_techniques"


# COMMAND ----------

from pyspark.sql.functions import when, datediff, dayofmonth, substring, month, year, lit, desc, concat_ws, regexp_replace, dense_rank, row_number, lag, max, lead, to_date, countDistinct, current_date, date_format, concat_ws, col, lpad, sum
from axapy.functions import telecharger_dataframe,telecharger_fichier,sauvegarder_dataframe_to_csv,telecharger_dossier
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from datetime import datetime
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSTRUCTION DE LA TABLE

# COMMAND ----------

# CNT = NBR de contrat
Output = (spark.read.format('DELTA').load('/mnt/base_sauv/Base_Epargne_Finale.DELTA').where('func_month="' + str(annee) + str(mois) + '"')
           #.filter(F.col("TOP_ONE")==1) # la base GP
           .withColumn("DATE_EFFET", to_date(col("DATE_EFFET"), "dd/MM/yyyy"))     
          .withColumn("TOP_AN",F.when(( F.col("CNT")==1)  &(year(col("DATE_EFFET"))==annee),1).otherwise(0) ) # Affaire nouvelle sur l'année

           .withColumn("qualite",F.when(F.col("TYPE_PRSN")=='PHY',F.lit('personne physique')).otherwise("personne morale")           )  
           .withColumn("Canal_distrib",F.when(F.col("RESEAU")=='GESTION PRIVEE DIRECTE',F.lit('Gestion Privée Directe'))
                                        .when(F.col("RESEAU")=='PARTENAIRE BANCAIRE',F.lit('Partenariats Bancaires'))
                                        .when(F.col("RESEAU")=='COURTIERS GENERALISTES',F.lit('Courtiers Généralistes'))
                                        .when(F.col("RESEAU")=='CGPI',F.lit('CGP'))
                                        .when(F.col("RESEAU")=='PARTENAIRE DISTRIBUTION',F.lit('Partenariats Distribution'))
                                        .when( (F.col("CANAL").isin(["GESTION PRIVEE"])) & (F.col("RESEAU").isin(["AGA","A2P","AEP"])),F.lit('Gestion Privée Réseau'))
                                        .when( (F.col("CANAL").isin(["RESEAU PROPRIETAIRE"])) & (F.col("RESEAU").isin(["AGA","A2P","AEP"])),F.lit('Réseau Propriétaire'))
                                        .when(F.col("RESEAU")=='AXA PARTENAIRE',F.lit('AXA PARTENAIRES'))
                                        .when(F.col("RESEAU")=='DIVERS AXA',F.lit('DIVERS AXA ASSURANCES'))
                                        .when(F.col("RESEAU")=='DOM TOM',F.lit('DOM-TOM'))
                                        .otherwise(F.lit("non déterminé"))           ) 

        .withColumn("reseau",F.when(F.col("RESEAU")=='AGA',F.lit('1 : AGA'))
                                        .when(F.col("RESEAU")=='A2P',F.lit('4 : A2P'))
                                        .when(F.col("RESEAU")=='AEP',F.lit('3 : Salarié'))  
                       .otherwise(F.lit(" "))           ) 
        
         .withColumn("SYSTEME",F.when(F.col("SYSTEME")=='NOVA',F.lit('ILIS')).otherwise(F.col("SYSTEME"))           )  


#################################
# On gère l'exception 0120931684

# Dans le portail AWS les contrats rattachés à ce portefeuille sont en MUT et ils devraient être en SA
#################################


 .withColumn("UJ",F.when(F.col("CODE_PORTEFEUILLE")=='0120931684',F.lit('SA')).otherwise(F.col("UJ"))           )  



#################################
# Forçage de Christophe

# Il ne veut pas de MUT pour les contrats NOVA(ILIS)
#################################

.withColumn("UJ",
                      F.when((F.col("SYSTEME").isin(['ILIS'])) &
                             (F.col("Canal_distrib").isin(["Courtiers Généralistes", "CGP"])) &
                             (F.col("UJ") == "MU"), F.lit('SA')).otherwise(F.col("UJ")))

)



# COMMAND ----------

# MAGIC %md
# MAGIC ## PREMIERE PARTIE

# COMMAND ----------

# A mettre dans l'onglet "Base", fichier "Donnees DF 20&annee. &mois..xlsx"


Output1 = (Output
 .filter(F.col("Canal_distrib").isin(["Gestion Privée Directe","Partenariats Bancaires","Courtiers Généralistes","CGP","Partenariats Distribution","Gestion Privée Réseau"]))
 .groupBy( F.col("SYSTEME"),F.col("UJ"),F.col("reseau"),F.col("Canal_distrib"),F.col("TYPE_SUPPORT"),F.col("qualite")  )
 .agg(
F.sum(F.col("Collecte_Brute_TOTALE")).alias("CA"),
F.sum(F.col("Prestation_TOTALE")).alias("Prestations"),
F.sum(F.col("PM_DEBUT")).alias("PM_début"),
F.sum(F.col("PM_FIN")).alias("PM_fin"),
F.sum(F.col("Arbitrage_Entrant")).alias("Arbitrages_entrée"),
F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrages_sortie"),
F.sum(F.col("CNT")).alias("cnt"),
F.sum(F.col("TOP_AN")).alias("an")  
 )
      
.select(F.col("SYSTEME"),F.col("UJ"),F.col("Canal_distrib"),F.col("reseau"),F.col("TYPE_SUPPORT").alias("support"),F.col("CA") ,F.col("Prestations"),
        F.col("PM_début"),F.col("PM_fin"),F.col("Arbitrages_entrée"),F.col("Arbitrages_sortie"),F.col("cnt") ,F.col("an"),F.col("qualite"))
 
          )


# COMMAND ----------

telecharger_dataframe(Output1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DEUXIEME PARTIE

# COMMAND ----------

# produits UNOFI car on isole la ligne dans la restitution

# A mettre dans l'onglet "Autres 2023", fichier "Donnees DF 20&annee. &mois..xlsx", cellule Q23 à Q25

# Output2
(Output.filter(F.col("CODE_PRODUIT").isin(["93984","90174","94264"]))
 .groupBy( F.col("TYPE_SUPPORT"), F.col("SYSTEME"))
 .agg(
     F.sum(F.col("PM_FIN")).alias("PM_FIN")
 )
 .display())



# COMMAND ----------

# MAGIC %md
# MAGIC ## TROISIEME PARTIE

# COMMAND ----------

## A mettre dans  onglet "data" dans le fichier data_au_&jour.&mois.20&annee

## On calul un nombre de contrat => CA > 0

 
Output3 = (Output.groupBy(
    F.col("SYSTEME"), F.col("UJ"), F.col("reseau"), F.col("Canal_distrib"), F.col("SITUATION_CLOTURE")
 )
 .agg(
    F.sum(F.col("Collecte_Brute_TOTALE")).alias("CA"),
    F.sum(F.col("Prestation_TOTALE")).alias("Prestations"),
    F.sum(F.col("PM_DEBUT")).alias("PM_début"),
    F.sum(F.col("PM_FIN")).alias("PM_fin"),
    F.sum(F.col("Arbitrage_Entrant")).alias("Arbitrages_entrée"),
    F.sum(F.col("Arbitrage_Sortant")).alias("Arbitrages_sortie"),

    F.sum( F.when(    (F.col("SITUATION_CLOTURE").isin(
                "Blocage informatique", "Echu", "Echu à régler", "Echu à traiter", "En attente de fonds",
                "En cours", "En erreur", "Réduit", "sans effet", "Sinistré", "suspendu","en cours",
                "Transfert Fourgous", "Transfert vers PER interne", "Transfert vers un contrat de même type (Pas d'imposition)") | (col("Collecte_Brute_TOTALE")>0)),      F.col("CNT")                                             ).otherwise(F.lit(0))
    
 ).alias("cnt"),
    F.sum(F.col("TOP_AN")).alias("an")


 )

 .select(
    F.col("SYSTEME"), F.col("UJ"), F.col("Canal_distrib"), F.col("reseau"), F.col("CA"),
    F.col("Prestations"), F.col("PM_début"), F.col("PM_fin"), F.col("Arbitrages_entrée"),
    F.col("Arbitrages_sortie"), F.col("cnt"), F.col("an")
 )
) 




