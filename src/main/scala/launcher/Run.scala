package launcher

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Run {
  // initialisation des variables globales
  var repertoireSourceDesFichiers    = ""
  var repertoireResultatsDesFichiers = ""

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    /** Assignation des paramètres */
    repertoireSourceDesFichiers = args(0)
    repertoireResultatsDesFichiers = args(1)
    val dateManuelleFichiers = args(2).replace("-", "") // ex: 2022-09-04 ou 20220904

    /** Creation de la SparkSession */
    logger.info("Lancement de l'application de calcul des stats")
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local")
      .setIfMissing("spark.sql.shuffle.partitions", "6")
    val spark = SparkSession.builder().appName("toip-calcul-stats").config(sparkConf).getOrCreate()

    try {

      /** Lancement du calcul des stats * */
      stats.Run.main(spark, repertoireSourceDesFichiers, dateManuelleFichiers)

      /** exposition Dataviz */
      expositiondataviz.Run.main(spark, repertoireResultatsDesFichiers)
      val message = "Fin du traitement complet de calcul des stats"
      logger.info(message)
    } finally {
      spark.stop
    }
  }
}
