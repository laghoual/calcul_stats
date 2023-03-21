package expositiondataviz

import controlefichiers.GestionFichiers.writeSingleFile1
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions => f}
import stats.ConstantesRepertoires.{DATAVIZSAGESREP, DATAVIZSTATSREP}
import utils.computeChecksum

object Run {
  // initialisation des variables globales
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(spark: SparkSession, repertoireResultatsDesFichiers: String): Unit = {

    ////////////////////////////////////
    /// CONF : Creation spark Session //
    ////////////////////////////////////
    // Creation de la Session
    logger.info("Debut de l'exposition Dataviz")
    ///////////////////////////////////

    /** ********* CREATION DES FICHIERS DATAVIZ ********
      */
    // lecture des tables et ecriture en csv dans le repertoire Dataviz
    var allStats = spark.table("stats").distinct.repartition(1)
    allStats = allStats.orderBy(allStats.columns.map { f.col }: _*)
    logger.info("ligne ecrites " + allStats.count())

    // ecriture stats
    logger.info(DATAVIZSTATSREP)
    writeSingleFile1(
      df = allStats,
      format = "csv",
      sc = spark.sparkContext,
      tmpFolder = repertoireResultatsDesFichiers + "/all/tmp/",
      filename = DATAVIZSTATSREP,
      saveMode = "overwrite",
      delimiterCaractere = ";"
    )

    // ecriture fichier sages
    var allSages = spark.table("sages").distinct.repartition(1)
    allSages = allSages.sort(allSages.columns.map { f.col }: _*)
    writeSingleFile1(
      df = allSages,
      format = "csv",
      sc = spark.sparkContext,
      tmpFolder = repertoireResultatsDesFichiers + "/all/tmp/",
      filename = DATAVIZSAGESREP,
      saveMode = "overwrite",
      delimiterCaractere = ";"
    )
    logger.info("Fin de l'exposition Dataviz")

    println(DATAVIZSTATSREP + " : " + computeChecksum(DATAVIZSTATSREP)) // 1cc07e1283b6bb02e5ee161158f0adf6
    println(DATAVIZSAGESREP + " : " + computeChecksum(DATAVIZSAGESREP)) // e5e63047c71dc2a29a72ff4cebec0138
  }
}
