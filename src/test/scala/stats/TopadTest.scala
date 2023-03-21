package stats

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import stats.Topad.procTopad

class TopadTest extends AnyFunSuite with BeforeAndAfter {
  // Given
  // Creation des variables de jeu d’essai
  var fichierTopad: DataFrame     = _
  var fichiertopadConf: DataFrame = _
  var spark: SparkSession         = _

  // Portion de code executee avant chaque test
  before {

    spark = SparkSession.builder().appName("SparkApp").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite

    // fichiers  en entree
    fichierTopad =
      spark.read.format("csv").option("delimiter", ";").option("header", "false").load("src/test/resources/topadsrc")
    fichiertopadConf = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("src/test/resources/topad_conf.csv")
  }

  test("Verification si le Dataframe en sortie n'est pas vide") {
    // WHEN
    val data = procTopad(spark, fichierTopad, fichiertopadConf)
    // THEN
    assert(!data.head(1).isEmpty)
  }
  test("Verification si le nombre de colonnes attendu est de 11") {
    // WHEN
    val data = procTopad(spark, fichierTopad, fichiertopadConf)
    // THEN
    assert(data.columns.length === 11)
  }
  test(
    "Verification si la colonne Code_UA contient des valeurs dont la longueur de champ est superieure ou égale à 7"
  ) {
    // WHEN
    val data = procTopad(spark, fichierTopad, fichiertopadConf)
    // THEN
    assert(data.filter(f.length(f.col("Code_UA")) < 7).count === 0)
  }

}
