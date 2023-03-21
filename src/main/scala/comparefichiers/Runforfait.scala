package comparefichiers

import org.apache.spark.sql.{functions => f}
import stats.Run.logger
import utils.Utils

object Runforfait {
  // val logger:Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val repertoireBaseSpark = args(0)
    val fromKnime           = args(1)
    val fromSpark           = args(2)
    val base                = args(3)
    val table               = args(4)

    ////////////////////////////////////
    /// CONF : Creation spark Session //
    ////////////////////////////////////
    // Creation de la Session
    val spark = Utils.creerSparkSession(repertoireBaseSpark)
    Utils.CreerEtSetBasePardefaut(base)
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
      .where(f.col("TicketId") === "4477743")

    rgid.show(false)

    // rgid.where(f.col("TicketId")==="4483899") 4506340
    val sprgid = spark
      .table(s"$table")
      .
      // FonctionsUtiles.readFileComplet(spark,null,fromSpark,",",headerStatus = true).
      withColumn("Attente_avant_mise_en_relation", f.col("Attente_avant_mise_en_relation").cast("double"))
      .withColumn("InitialStartDateTime", f.date_format(f.col("InitialStartDateTime"), "yyyy-MM-dd HH:mm"))
      .withColumnRenamed("StartDateTime", "NewStartDateTime")
      .withColumn("SAGES", f.trim(f.col("SAGES")))
      .where(f.col("TicketId") === "4477743")

    sprgid.show(false)

    val ListDropped = List("bloc_name", "InitialStartDateTime", "NewStartDateTime")
    calculerDifferencesDfPrintln(
      rgid.drop(ListDropped: _*).orderBy(f.asc("TicketId")),
      sprgid.drop(ListDropped: _*).orderBy(f.asc("TicketId")),
      differencesParColonne = false,
      ""
    )

    // bloc name 4478058 4477973
    // rgid.printSchema
    // controles decroches
    // rgid.join(sprgid,Seq("TicketId"),"leftanti").show(false)
    logger.info("Fin de la session")
    spark.stop()

  }
}
