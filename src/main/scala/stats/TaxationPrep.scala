package stats

//-1  --1
import org.apache.log4j.Logger
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TaxationPrep {

  // creation de la classe permettant de gérer les logs
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def procTaxation(spark: SparkSession, taxation: DataFrame): DataFrame = {

    logger.info("Traitement Taxation")

    // Traitement de controle SVI
    val listColumns = List(
      "InitialStartDateTime",
      "TicketId",
      "CalledNumber",
      "CallingNumber",
      "CallType",
      "EffectiveCallDuration",
      "StartDateTime",
      "InternFacilities",
      "ChargedNumber",
      "CostCenter",
      "WaitingDuration"
    )

    if (!controleColonnesDf(taxation, listColumns)) {
      logger.info("Le fichier taxation n'est pas conforme")
      spark.stop()
    }

    // Traitement du fichier Taxation
    val df_taxation = taxation.select(
      f.date_format(f.col("InitialStartDateTime"), "yyyy-MM-dd HH:mm:ss.SSS").as("InitialStartDateTime"),
      f.col("TicketId"),
      f.col("CalledNumber"),
      f.col("CallingNumber"),
      f.col("CallType"),
      f.col("EffectiveCallDuration").cast("Integer"),
      f.date_format(f.col("StartDateTime"), "yyyy-MM-dd HH:mm:ss.SSS").as("StartDateTime"),
      f.col("InternFacilities"),
      f.col("ChargedNumber"),
      f.col("CostCenter"),
      f.col("WaitingDuration")
    )

    df_taxation

  }

}
