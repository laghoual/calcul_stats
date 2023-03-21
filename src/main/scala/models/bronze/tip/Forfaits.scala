package models.bronze.tip

import datawrap.core.{Materialization, SparkModel, Variables}
import org.apache.spark.sql.{DataFrame, functions => f}
import stats.ConstantesColonnes.{
  COLSFORFAITFINAL,
  COLSFORFAITFINALINTERNATIONAL,
  COLSFORFAITRECUINTERNATIONAL,
  COLSREGLEGESTIONFINAL
}
import utils.FonctionsUtiles

import scala.collection.mutable.ListBuffer

object Forfaits extends SparkModel {

  override val declaredVariables: Set[String] = Set("jour")

  override val partitions: Seq[String] = Seq("jour")

  override val inputs: Set[SparkModel] = Set(ReglesGestion, NumTaxationSource, NumTaxationInternationalSource)

  override val materialization: Materialization = Materialization.FILE("ORC")

  override protected def compute(implicit variables: Variables): DataFrame = {
    // *********************************************************** Appel France *******************************************************//

    // Filtrer les appels par longueur du numéro appelant
    val reglesGestion = ReglesGestion
      .withColumn("len", f.length(f.col("CalledNumber")))
      .withColumn("Date", f.col("InitialStartDateTime").substr(0, 10))
    // longueur 10
    val reglesGestionLen10 = reglesGestion.where(f.length(f.col("CalledNumber")) === 10)
    // longueur 4
    val reglesGestionLen4 = reglesGestion.where(f.length(f.col("CalledNumber")) === 4)
    // longueur 8
    val reglesGestionLen8 = reglesGestion.where(f.length(f.col("CalledNumber")) === 8)
    // longueur Court
    val reglesGestionCourt = reglesGestion.where(
      !(f.length(f.col("CalledNumber")) === 8) && !(f.length(f.col("CalledNumber")) === 4)
        && f.length(f.col("CalledNumber")) < 10
    )
    // longueur > 10
    val reglesGestionInternational        = reglesGestion.where(f.length(f.col("CalledNumber")) > 10)
    val numerotationTaxation              = NumTaxationSource
    val numerotationTaxationInternational = NumTaxationInternationalSource

    // ***************************************************** Node 202  sur KNIME **************************************************//
    // Récupérer les calls qui commencent par "08" : Calls spéciaux
    val reglesGestionSt08 = reglesGestionLen10.where(f.col("CalledNumber").startsWith("08"))
    val reglesGestion08   = gestionCallsSpeciaux(reglesGestionSt08, numerotationTaxation, 4, "Numéros spéciaux")
    // ****************************************************************************************************************************//

    // ****************************************************** Node 203  sur KNIME ****************************************************//
    // Récupérer les calls qui commencent par "07" : Appel national
    val reglesGestionSt07 = reglesGestionLen10.where(f.col("CalledNumber").startsWith("07"))
    val reglesGestion07   = gestionCallsSpeciaux(reglesGestionSt07, numerotationTaxation, 3, "National")
    // logger.info(s" count reglesGestion07 $cnt")
    // **************************************************************************************************************************************//

    // ****************************************************** Node 208 : Other france ****************************************************//

    // Récupérer les calls qui ne commencent pas par "07"ou 08 : Appel national toujours
    val reglesGestionOtherFranceSt =
      reglesGestionLen10.where(!f.col("CalledNumber").startsWith("07") && !f.col("CalledNumber").startsWith("08"))
    val reglesGestionOtherFrance = gestionCallsSpeciaux(reglesGestionOtherFranceSt, numerotationTaxation, 2, "National")
    // FonctionsUtiles.writeTableOrc(reglesGestionOtherFrance,"rg_otherfrance_node208")
    // sys.exit(0)
    // *****************************************************************************************************************************************//
    // ****************************************************** Node 205 : Numéro courts france ****************************************************//

    // Récupérer les données du pays suite à la jointure avec le fichier des indicatifs
    var reglesGestionCourts = gestionCallsSpeciaux(reglesGestionLen4, numerotationTaxation, 2, "N° courts")
    reglesGestionCourts = reglesGestionCourts.withColumn(
      "calledNumber_externe_interne",
      f.when(f.col("Type_appel") === "Appel émis", f.lit("Externe")).otherwise(f.col("calledNumber_externe_interne"))
    )
    // FonctionsUtiles.writeTableOrc(reglesGestionCourts,"reglesgestioncourts_205")
    // sys.exit(0)
    // *****************************************************************************************************************************************//

    // ****************************************************** Cas len 8 ( num court urgence )  215 ****************************************************//
    // Pour le cas des nunméro court urgence
    val reglesGestion08NumCourtUrgence =
      gestionCourtSpeciaux(reglesGestionLen8, "Forfait", "Numéros internes", "", "")
    // *****************************************************************************************************************************************//

    // ****************************************************** Cas court Service   node 206 ****************************************************//
    val reglesGestionCourtServiceFiltered = reglesGestionCourt.where(
      f.col("CalledNumber") === "15" ||
        f.col("CalledNumber") === "17" || f.col("CalledNumber") === "18"
    )
    val reglesGestionCourtService =
      gestionCourtSpeciaux(reglesGestionCourtServiceFiltered, "Forfait", "Numéros d'urgence", "", "")

    // ****************************************** Court autre 214 ***************************************************************************//
    val reglesGestionCourtAutreFiltered = reglesGestionCourt.where(
      !(f.col("CalledNumber") === "15" ||
        f.col("CalledNumber") === "17" || f.col("CalledNumber") === "18")
    )
    // sys.exit(0)
    val reglesGestionCourtAutre =
      gestionCourtSpeciaux(reglesGestionCourtAutreFiltered, "Forfait", "Numéros internes", "", "")
    // FonctionsUtiles.writeTableOrc(reglesGestionCourtAutre,"reglesgestioncourtautre_214")
    // sys.exit(0)
    /** ************************************************* International
      * *****************************************************************
      */
    // Traitement du fichier taxation qui contient les indicatifs : faire un regroupemet par indicatif
    // Ce regroupement permet de gérer le cas ou un indicatif appartient  plusieurs pays
    val numerotationTaxationInternationalGrouped = numerotationTaxationInternational
      .groupBy("Indicatif")
      .agg(f.concat_ws(", ", f.collect_list(f.col("Pays"))).alias("Pays"))

    // node 204
    // Récupérer les cas émis
    val internationalEmisSrc = reglesGestionInternational.where(f.col("Type_appel") === "Appel émis")
    val internationalEmis =
      internationalEmisSrc.withColumn("Indicatif_numero", f.regexp_replace(f.col("CalledNumber"), "^00", ""))
    // split des indicateurs à 2
    // split des indicateurs à 3
    val internationalEmisIndicLen2 =
      internationalEmis.withColumn("Indicatif_numero", f.substring(f.col("Indicatif_numero"), 0, 2))
    val internationalEmisIndicLen3 =
      internationalEmis.withColumn("Indicatif_numero", f.substring(f.col("Indicatif_numero"), 0, 3))
    // union des 2 sub : node 199
    val internationalEmisIndics =
      FonctionsUtiles.unionLeftDFwithRightDF(internationalEmisIndicLen2, internationalEmisIndicLen3)
    // node 193-------------------------------
    val internationalEmisIndicsNumeros = internationalEmisIndics
      .join(numerotationTaxationInternationalGrouped, f.col("Indicatif_numero") === f.col("Indicatif"), "leftouter")
      .drop("Indicatif")
    // node 183
    val internationalEmisIndicsAllNumeros = internationalEmisIndicsNumeros.join(
      numerotationTaxation.select("Indicatif"),
      f.col("Indicatif_numero") === f.col("Indicatif"),
      "inner"
    )

    val internationalEmisIndicsAllNumerosAnti = internationalEmisIndicsNumeros.join(
      numerotationTaxation.select("Indicatif"),
      f.col("Indicatif_numero") === f.col("Indicatif"),
      "leftouter"
    )
    // internationalEmisIndicsAllNumerosAnti.show(false)

    // appels emis forfait
    val internationalEmisForfait = internationalEmisIndicsAllNumeros
      .withColumn("Appel_facturable", f.lit("Forfait"))
      .selectExpr(COLSFORFAITFINALINTERNATIONAL: _*)

    // appels emis hors forfait
    val internationalEmisIndicsAllNumerosGrouped = internationalEmisIndicsAllNumeros.groupBy(f.col("TicketId")).count()

    // ajustement de la liste
    val myList = ListBuffer(COLSREGLEGESTIONFINAL: _*)
    myList -= ("Aboutissement_SVI", "Motif_appel")
    val COLSFORFAITFINALINTERNATIONALGROUPED = myList.toList
    // international anti
    var internationalEmisHorsForfait = internationalEmisIndicsNumeros
      .join(internationalEmisIndicsAllNumerosGrouped, Seq("TicketId"), "leftanti")
      .groupBy(COLSFORFAITFINALINTERNATIONALGROUPED map f.col: _*)
      .count()
      .drop("count")

    internationalEmisHorsForfait = internationalEmisHorsForfait
      .withColumn("Appel_facturable", f.lit("Hors forfait"))
      .withColumn("Indicatif_numero", f.lit(null))
      .withColumn("Pays", f.lit(null))
      .withColumn("Aboutissement_SVI", f.lit(null))
      .withColumn("Motif_appel", f.lit(null))
      .withColumn("len", f.lit(null))

    // .selectExpr(COLSFORFAITFINALINTERNATIONAL:_*)

    // println(COLSFORFAITFINALINTERNATIONALGROUPED)
    // sys.exit(0)
    // international Appel reçus
    val internationalRecusSrc =
      reglesGestionInternational
        .where(f.col("Type_appel") === "Appel reçu")
        .selectExpr(COLSFORFAITRECUINTERNATIONAL: _*)
    val internationalRecus = internationalRecusSrc
      .withColumn("Indicatif_numero", f.lit(null))
      .withColumn("Pays", f.lit(null))
      .withColumn("Appel_facturable", f.lit(null))
      .selectExpr(COLSFORFAITFINALINTERNATIONAL: _*)

    // sys.exit(0)

    // ************************************************ Union International ****************************************************/
    // les unions
    val internationalRecuForfait = FonctionsUtiles.unionLeftDFwithRightDF(internationalRecus, internationalEmisForfait)
    val internationalUnion =
      FonctionsUtiles.unionLeftDFwithRightDF(internationalRecuForfait, internationalEmisHorsForfait)
    // ajout de la colonne Appel vers
    val internationalUnionWithAppels = internationalUnion
      .withColumn("Appel_vers", f.lit("International"))
      .
      // drop("len").
      selectExpr(COLSFORFAITFINALINTERNATIONAL :+ "Appel_vers": _*)
      .drop("len")

    // ************************************************** Jointure finale : Union ***************************************************//
    // Union finale
    val rgAnd08And07               = FonctionsUtiles.unionLeftDFwithRightDF(reglesGestion08, reglesGestion07)
    val rgAnd08And07AndOtherFrance = FonctionsUtiles.unionLeftDFwithRightDF(rgAnd08And07, reglesGestionOtherFrance)
    val rgAnd08And07AndOtherAndCourtFrance =
      FonctionsUtiles.unionLeftDFwithRightDF(rgAnd08And07AndOtherFrance, reglesGestionCourts)
    val rgAnd08And07AndOtherAndCourtsUrgence =
      FonctionsUtiles.unionLeftDFwithRightDF(rgAnd08And07AndOtherAndCourtFrance, reglesGestion08NumCourtUrgence)
    val rgAnd08And07AndOtherAndCourtsUrgenceService =
      FonctionsUtiles.unionLeftDFwithRightDF(rgAnd08And07AndOtherAndCourtsUrgence, reglesGestionCourtService)
    val rgAnd08And07AndOtherAndCourtsUrgenceServiceAutre =
      FonctionsUtiles.unionLeftDFwithRightDF(rgAnd08And07AndOtherAndCourtsUrgenceService, reglesGestionCourtAutre)

    val rgPrepareForUnion = rgAnd08And07AndOtherAndCourtsUrgenceServiceAutre
      .withColumn("Indicatif_numero", f.lit(null))
      .withColumn("Pays", f.lit(null))

    // union avec international
    val rGAndInternational = FonctionsUtiles.unionLeftDFwithRightDF(internationalUnionWithAppels, rgPrepareForUnion)

    val forfaitHorsHorfait = rGAndInternational

    forfaitHorsHorfait.withColumn("jour", f.lit(variables("jour")))
  }

  def gestionCallsSpeciaux(
      dfGestionSrc: DataFrame,
      numerotationTaxation: DataFrame,
      longueurChamp: Int,
      ContenuAppelVers: String
  ): DataFrame = {
    var dfGestion = dfGestionSrc.withColumn("new", f.substring(f.col("CalledNumber"), 0, longueurChamp))

    // Jointure entre les données récupérer par le calcul des règles de gestion et le fichier qui contient les indicatifs
    dfGestion = dfGestion
      .join(numerotationTaxation, f.col("new") === f.col("Debut") && f.col("len") === f.col("Longueur"), "leftouter")
      .withColumn(
        "Appel_facturable",
        f.when(f.col("Debut").isNotNull, "Forfait")
          .otherwise("Hors forfait")
      )

    val dfGestionReturned = dfGestion
      .withColumn("Appel_vers", f.lit(ContenuAppelVers))
      .selectExpr(COLSFORFAITFINAL: _*)
    dfGestionReturned.selectExpr(COLSFORFAITFINAL: _*)
  }

  def gestionCourtSpeciaux(
      reglesGestionLen8: DataFrame,
      appel_facturable: String,
      Appel_vers: String,
      Pays: String,
      Indicatif: String
  ): DataFrame = {
    reglesGestionLen8
      .withColumn("Appel_facturable", f.lit(appel_facturable))
      .withColumn("Appel_vers", f.lit(Appel_vers))
      // .withColumn("Pays",f.lit(Pays))
      // .withColumn("Indicatif",f.lit(Indicatif))
      .drop("len")
      .selectExpr(COLSFORFAITFINAL: _*)
  }
}
