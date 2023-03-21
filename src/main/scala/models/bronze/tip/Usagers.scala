package models.bronze.tip

import datawrap.core.{Materialization, SparkModel, Variables}
import models.bronze.tip_topad.Topad
import org.apache.spark.sql.{DataFrame, functions => f}
import stats.controleColonnesDf

object Usagers extends SparkModel {

  override val declaredVariables: Set[String] = Set("jour")

  override val partitions: Seq[String] = Seq("jour")

  override val inputs: Set[SparkModel] = Set(Topad, UsagersSource)

  override val materialization: Materialization = Materialization.FILE("ORC")

  override protected def compute(implicit variables: Variables): DataFrame = {
    // Traitement de controle SVI
    val listColumns = List("Internal Number", "Cost Center")

    if (!controleColonnesDf(UsagersSource, listColumns)) {
      logger.info("Le fichier taxation n'est pas conforme")
      spark.stop()
    }

    // le fichier usager qui va nous aider à récupérer le code sages
    // renommage
    val usagerSelected = UsagersSource
      .withColumnRenamed("Internal Number", "Internal_Number")
      .withColumnRenamed("Cost Center", "Cost_Center")
      .selectExpr("Internal_Number", "Cost_Center")

    // traitement du fichier usagers
    val usagerCleaned = usagerSelected
      .withColumn("cost_test_replaced", f.regexp_replace(f.col("Cost_Center"), " ", ""))
      .withColumn("Cost_Center", f.substring(f.col("cost_test_replaced"), 0, 7))
      .drop("cost_test_replaced")

    val topadUa = Topad.selectExpr("Code_UA")
    // jointure avec TOPAD
    val usagerTopad = usagerCleaned.join(topadUa, f.col("Code_UA") === f.col("Cost_Center"), "inner")

    usagerTopad.withColumn("jour", f.lit(variables("jour")))
  }

}
