package comparefichiers
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Runnonregression {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("lancement de la comparaison")
    // variables
    val cheminFichierRecette = args(0)
    val cheminFichierBatch   = args(1)
    logger.info("Fin du batch")
    // Creation de la Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("nonreg")
      .config("spark.sql.shuffle.partitions", "6")
      .getOrCreate()

    // fichiers de comparaison
    val recette = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(cheminFichierRecette)
    val batch   = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(cheminFichierBatch)
    // fonction de comparaison de la non reg
    calculerDifferencesDfPrintln(recette, batch, differencesParColonne = false, "")
    logger.info("Fin du batch")
  }
}
