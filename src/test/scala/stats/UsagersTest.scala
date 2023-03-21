package stats

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import stats.Usagers.procUsagers

class UsagersTest extends AnyFunSuite with BeforeAndAfter {
  // Given
  // Creation des variables de jeu d’essai
  var fichierTopad: DataFrame  = _
  var fichierUsager: DataFrame = _
  var spark: SparkSession      = _

  // Portion de code executee avant chaque test
  before {

    spark = SparkSession.builder().appName("SparkApp").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite

    // data_raw est le fichier data en entree
    fichierTopad = spark.read
      .format("csv")
      .option("delimiter", "$")
      .option("header", "true")
      .load("src/test/resources/topad_transformed.csv")
    fichierUsager = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("src/test/resources/usagers_src.csv")

  }

  test("Verification si le Dataframe en sortie de procUsagers n est pas vide") {
    // WHEN
    val data = procUsagers(spark, fichierUsager, fichierTopad)
    // THEN
    assert(!data.head(1).isEmpty)
  }
  test("Verification si le nombre de colonnes attendu a la fin de procUsagers est de 3") {
    // WHEN
    val data = procUsagers(spark, fichierUsager, fichierTopad)
    // THEN
    assert(data.columns.length === 3)
  }
  test(
    "Verification si la colonne Code_UA contient des valeurs dont la longueur de champ est superieure ou egale à 7"
  ) {
    // WHEN
    val data = procUsagers(spark, fichierUsager, fichierTopad)
    // THEN
    assert(data.filter(f.length(f.col("Code_UA")) < 7).count === 0)
  }

}
