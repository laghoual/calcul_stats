package stats

import org.apache.log4j.Logger
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Usagers {
  // creation de la classe permettant de gérer les logs
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def procUsagers(spark: SparkSession, usagerDf: DataFrame, topad: DataFrame): DataFrame = {
    // Traitement de controle SVI
    val listColumns = List("Internal Number", "Cost Center")

    val usagerSrc = usagerDf
    logger.info("debut usager")
    if (!controleColonnesDf(usagerSrc, listColumns)) {
      logger.info("Le fichier taxation n'est pas conforme")
      spark.stop()
    }

    // le fichier usager qui va nous aider à récupérer le code sages
    // renomage
    val usagerSelected = usagerSrc
      .withColumnRenamed("Internal Number", "Internal_Number")
      .withColumnRenamed("Cost Center", "Cost_Center")
      .selectExpr("Internal_Number", "Cost_Center")

    // traitement du fichier usagers
    val usagerCleaned = usagerSelected
      .withColumn("cost_test_replaced", f.regexp_replace(f.col("Cost_Center"), " ", ""))
      .withColumn("Cost_Center", f.substring(f.col("cost_test_replaced"), 0, 7))
      .drop("cost_test_replaced")

    val topadUa = topad.selectExpr("Code_UA")
    // jointure avec TOPAD
    val usagerTopad = usagerCleaned.join(topadUa, f.col("Code_UA") === f.col("Cost_Center"), "inner")

    logger.info("fin usager")

    usagerTopad
  }
}
