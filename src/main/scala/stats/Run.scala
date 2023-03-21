package stats

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import stats.ConstantesRepertoires._
import utils.FonctionsUtiles

object Run {

  // creation de la classe permettant de gérer les logs
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(spark: SparkSession, repertoireSourceDesFichiers: String, dateManuelleFichiers: String): Unit = {

    val taxationFilePath =
      s"$repertoireSourceDesFichiers/taxation/all/ResultatDGFIP_Taxation_DailyStats_$dateManuelleFichiers.csv"
    val sviFilePath     = s"$repertoireSourceDesFichiers/svi/all/ResultatDGFIP_DailyStats_$dateManuelleFichiers.csv"
    val svinodeFilePath = s"$repertoireSourceDesFichiers/svinode/all/ResultatDGFIP_NodeNames_$dateManuelleFichiers.csv"
    val usagersFilePath = s"$repertoireSourceDesFichiers/usagers/all/ResultatDGFIP_Usagers_$dateManuelleFichiers.csv"

    logger.info("Debut du calcul des stats")

    /** ********* lancement du calcul des stats ********
      */

    // ********************************* TAXATION ***************************************************//
    // Lecture et traitement du fichier TAXATION
    val taxationSrc =
      FonctionsUtiles.readFileComplet(spark, null, taxationFilePath, ",", headerStatus = true)
    val taxation = TaxationPrep.procTaxation(spark, taxationSrc)

    // *********************************     SVI   *************************************************//
    // Lecture et traitement du fichier SVI et SVI node
    val sviSrc     = FonctionsUtiles.readFile(spark, header = true, sviFilePath, ",", "csv")
    val sviNodeSrc = FonctionsUtiles.readFile(spark, header = true, svinodeFilePath, ",", "csv")
    val svi        = SviPrep.procSVI(spark, sviSrc, sviNodeSrc).localCheckpoint()

    // ********************************* Jointure Floue *************************************************//
    // La jointure floue
    val SviTaxationJoined = SviTaxJoinFloue.procJointureFloue(spark, svi, taxation).localCheckpoint()

    // *********************************** Regles de gestion *********************************************//
    // Module: Regles de Gestion
    logger.info("Début du traitement ReglesGestions")
    val reglesGestion = ReglesGestion.procReglesGestion(SviTaxationJoined).localCheckpoint()

    // ************************************* Forfait HorsForfait ******************************************//
    // Lecture et traitement des fichiers numerotation taxation
    val numTaxation = FonctionsUtiles.readFileComplet(spark, null, REGLENUMEROTAXATIONREP, ",", headerStatus = true)
    val NumTaxationInternational =
      FonctionsUtiles.readFileComplet(spark, null, REGLENUMEROTAXATIONINTREP, ",", headerStatus = true)

    val forfaits = Forfaits.procForfaits(spark, reglesGestion, numTaxation, NumTaxationInternational)
    FonctionsUtiles.writeTableOrc(forfaits, "forfaits")

    // ***************************************** TOPAD ***************************************//
    val topadSrc     = FonctionsUtiles.readFileComplet(spark, null, TOPADREP, ",", headerStatus = false)
    val topadConfSrc = FonctionsUtiles.readFileComplet(spark, null, TOPADCONFREP, ",", headerStatus = true)
    val topad        = Topad.procTopad(spark, topadSrc, topadConfSrc)

    // ***************************************** Usagers ***************************************//
    val usagersSrc   = FonctionsUtiles.readFileComplet(spark, null, usagersFilePath, ",", headerStatus = true)
    val usagersTopad = Usagers.procUsagers(spark, usagersSrc, topad)

    // ***************************************** Croisements finaux ***************************************//
    Croisements.procCroisements(usagersTopad, forfaits, topad)
    logger.info("Fin traitement de calcul")
  }

}
