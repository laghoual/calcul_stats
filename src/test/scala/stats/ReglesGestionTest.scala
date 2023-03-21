package stats

//package stats

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import stats.ReglesGestion.procReglesGestion

class ReglesGestionTest extends AnyFunSuite with BeforeAndAfter {
  // Given
  // Creation des variables de jeu d’essai
  var fichierDeTaxation: DataFrame        = _
  var fichierTransFromTaxation: DataFrame = _

  // Portion de code executee avant chaque test
  before {

    val spark = SparkSession.builder().appName("SparkApp").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite

    // fichierDeTaxation est le fichier data en entree
    fichierDeTaxation = spark.read
      .format("csv")
      .option("delimiter", "$")
      .option("header", "true")
      .load("src/test/resources/sviTaxJoined.csv")
  }

  test("Verification si le Dataframe en sortie de procReglesGestion n'est pas vide") {
    // WHEN
    val data = procReglesGestion(fichierDeTaxation)
    // THEN
    assert(!data.head(1).isEmpty)
  }
  test("Verification si le nombre de colonnes attendu est de 47") {
    // WHEN
    val data = procReglesGestion(fichierDeTaxation)
    // THEN
    assert(data.columns.length === 47)
  }
  test("Verification si la colonne appel_emis a une des valeurs  0 ou 1") {
    // WHEN
    val data = procReglesGestion(fichierDeTaxation)
    // THEN
    assert(data.select("Appels_emis").where(f.col("Appels_emis") === "1" || f.col("Appels_emis") === "0").count != 0)
  }

}
