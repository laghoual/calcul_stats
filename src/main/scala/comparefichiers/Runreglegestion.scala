package comparefichiers
import org.apache.spark.sql.{functions => f}
import stats.Run.logger
import utils.{FonctionsUtiles, Utils}

object Runreglegestion {
  // val logger:Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val repertoireBaseSpark = args(0)
    val fromKnime           = args(1)
    val fromSpark           = args(2)

    ////////////////////////////////////
    /// CONF : Creation spark Session //
    ////////////////////////////////////
    // Creation de la Session
    val spark = Utils.creerSparkSession(repertoireBaseSpark)
    logger.info("Creation de la session Spark de l'application")

    // calcul des differences

    val rgid = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .load(fromKnime)
      .withColumn("bloc_duration", f.col("bloc_duration").cast("double"))
      .withColumn("duration", f.col("duration").cast("double"))
      .withColumn("Appel_transfere", f.col("Appel_transfere").cast("double"))
      .withColumn("Appels_transferes", f.col("Appels_transferes"))
      .withColumn("bloc_name", f.regexp_replace(f.col("bloc_name"), "\"", ""))
      .withColumn("Attente_avant_mise_en_relation", f.col("Attente_avant_mise_en_relation").cast("double"))
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), "T", " "))
      .where(f.col("TicketId") === "4482289")
      .withColumn("NewStartDateTime", f.regexp_replace(f.col("NewStartDateTime"), "T", " "))

    rgid.show(false)

    // rgid.where(f.col("TicketId")==="4482289") 4482289 4510954
    // donnees spark
    val sprgid = FonctionsUtiles
      .readFileComplet(spark, null, fromSpark, ",", headerStatus = true)
      .withColumn("Attente_avant_mise_en_relation", f.col("Attente_avant_mise_en_relation").cast("double"))
      .
      // withColumn("InitialStartDateTime",date_format(f.col("InitialStartDateTime"),"yyyy-MM-dd HH:mm:SSS")).
      withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), ".000", ""))
      .withColumnRenamed("StartDateTime", "NewStartDateTime")
      .where(f.col("TicketId") === "4482289")
      .withColumn("NewStartDateTime", f.regexp_replace(f.col("NewStartDateTime"), ".000", ""))
    // withColumn("NewStartDateTime",date_format(f.col("NewStartDateTime"),"yyyy-MM-dd HH:mm:SS"))

    sprgid.show(false)

    val ListDropped = List("bloc_name")
    calculerDifferencesDfPrintln(
      rgid.drop(ListDropped: _*).orderBy(f.asc("TicketId")),
      sprgid.drop(ListDropped: _*).orderBy(f.asc("TicketId")),
      differencesParColonne = false,
      ""
    )

    logger.info("Fin de la session")

    spark.stop()

  }
}
