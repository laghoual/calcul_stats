package models.bronze.tip

import datawrap.core.{Materialization, SparkModel, Variables}
import models.bronze.tip_topad.Topad
import org.apache.spark.sql.{DataFrame, functions => f}
import stats.ConstantesColonnes.COLSSAGESTRANSFORMED
import stats.creationColsDate

object Croisements extends SparkModel {

  override val declaredVariables: Set[String] = Set("jour")

  override val partitions: Seq[String] = Seq("jour")

  override val inputs: Set[SparkModel] = Set(Forfaits, Topad, Usagers)

  override val materialization: Materialization = Materialization.FILE("ORC")

  override protected def compute(implicit variables: Variables): DataFrame = {
    // Récupérer les données avec les bons endcause

    // FonctionsUtiles.writeTableOrc(forfaits,"forfaitsfull")

    // exclusion d'une partie des données
    val forfaitFiltered = Forfaits
      .where(f.col("jour") === f.lit(variables("jour")))
      .where(
        (f.col("endcause") === "NOT ENOUGH LICENSE") or
          ((f.col("CallType") === "9") && (f.col("endcause") === "TRANSFERRED"))
      )
    val forfaitByEndcause =
      Forfaits.join(forfaitFiltered, Seq("TicketId", "InitialStartDateTime", "endcause", "CallType"), "leftanti")

    val usagersFiltered = Usagers.where(f.col("jour") === f.lit(variables("jour")))

    // Jointure entre les données Forfait hors forfait et la jointure topad / usagers
    val forfaitsUsagersTopad =
      forfaitByEndcause.join(usagersFiltered, f.col("ChargedNumber") === f.col("Internal_Number"), "leftouter")
    // logger.info("forfaitsusagerTopadleft------"+forfaitsUsagersTopad.count())

    // filtres SAGES
    val sagesTransformed = forfaitsUsagersTopad
      .withColumn("bloc_sages_replace", f.regexp_replace(f.col("SAGES"), " ", ""))
      .withColumn("bloc_sages_sub", f.substring(f.col("bloc_sages_replace"), 0, 7))
      .withColumn(
        "SAGES",
        f.when(f.col("Cost_Center").isNull || f.col("Cost_Center") === "", f.col("bloc_sages_sub"))
          .otherwise(f.col("Cost_Center"))
      )

    val sagesFiltered =
      sagesTransformed.where(!(f.col("SAGES").isNull || f.col("SAGES") === "")).selectExpr(COLSSAGESTRANSFORMED: _*)
    // FonctionsUtiles.writeTableOrc(sagesFiltered,"croisementmid")
    // sys.exit(0)

    // croisement de nouveau avec topad et les donnees svitaxtopad
    val forfaitsUsagersTopadSages = sagesFiltered
      .withColumn("SAGES", f.trim(f.col("SAGES")))
      .join(Topad, f.col("SAGES") <=> f.col("Code_UA"), "inner")

    // ajout des colonnes dates
    val forfaitsUsagersTopadDate = creationColsDate(forfaitsUsagersTopadSages)

    // ajout de la colonne service
    val forfaitsUsagersTopadSagesDateService = forfaitsUsagersTopadDate
      .withColumn("Service", f.concat(f.col("Bureau_court"), f.lit(" "), f.col("Bureau_2")))
      .withColumn(
        "Attente_avant_mise_en_relation",
        f.bround(f.col("Attente_avant_mise_en_relation").cast("double")).cast("int")
      )

    forfaitsUsagersTopadSagesDateService.withColumn("jour", f.lit(variables("jour")))
//    // selection des colonnes pour Dataviz
//    val stats = forfaitsUsagersTopadSagesDateService.selectExpr(COLSFINAL: _*)
//    stats.write.mode("overwrite").format("orc").saveAsTable("stats")
//    logger.info("lignes dans stats:  " + stats.count)
//    // selection des sages
//    val sagesData = forfaitsUsagersTopadSagesDateService.selectExpr("SAGES").distinct
//    sagesData
  }

}
