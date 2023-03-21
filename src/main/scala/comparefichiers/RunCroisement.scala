package comparefichiers

import stats.Run.logger
//import org.apache.log4j.Logger
import org.apache.spark.sql.{functions => f}
import utils.{FonctionsUtiles, Utils}

object RunCroisement {
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
    val forfaits = spark
      .table("stats")
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), " ", "T"))
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), ".000", ""))

    FonctionsUtiles.writeTableCsv(
      forfaits,
      "iso_stats_dtk",
      "C:\\Users\\ymikponhoue01\\Documents\\spark_env\\toip\\donnees\\resultats\\",
      ","
    )

    val sagess = spark.table("sagesData")
    FonctionsUtiles.writeTableCsv(
      sagess,
      "iso_sages_dtk",
      "C:\\Users\\ymikponhoue01\\Documents\\spark_env\\toip\\donnees\\resultats\\",
      ","
    )

    val rgid = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .load(fromKnime)
      .
      // withColumn("Appel_transfere",col("Appel_transfere").cast("double")).
      withColumn("Appels_transferes", f.col("Appels_transferes"))
      .withColumn("Attente_avant_mise_en_relation", f.col("Attente_avant_mise_en_relation").cast("double"))
    // withColumn("InitialStartDateTime",regexp_replace(f.col("InitialStartDateTime"),"T"," "))
    // .where(f.col("CalledNumber")==="0628209917")

    rgid.show(false)

    val sprgid = spark
      .table(s"$table")
      .
      // FonctionsUtiles.readFileComplet(spark,null,fromSpark,",",headerStatus = true).
      withColumnRenamed("Appels_raccroches", "Appels_raccrohes")
      .withColumn("Attente_avant_mise_en_relation", f.col("Attente_avant_mise_en_relation").cast("double"))
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), " ", "T"))
      // .withColumn("InitialStartDateTime",regexp_replace(f.col("InitialStartDateTime"),":00",""))
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), ".000", ""))

    // withColumn("InitialStartDateTime",date_format(f.col("InitialStartDateTime"),"yyyy-MM-dd HH:mm")).
    // .withColumn("Lib_hour",regexp_replace(f.col("Lib_hour"),"-"," - "))

    // .where(f.col("CalledNumber")==="0628209917")

    sprgid.show(false)

    val ListDropped = List("InitialStartDateTime")
    calculerDifferencesDfPrintln(
      rgid.drop(ListDropped: _*).orderBy(f.asc("SAGES")),
      sprgid.drop(ListDropped: _*).orderBy(f.asc("SAGES")),
      differencesParColonne = false,
      ""
    )

    logger.info("Fin de la session")
    spark.stop()

  }
}
