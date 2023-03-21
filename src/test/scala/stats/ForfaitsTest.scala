package stats

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import stats.Forfaits.procForfaits
import utils.FonctionsUtiles

class ForfaitsTest extends AnyFunSuite with BeforeAndAfter {
  // Given
  // Creation des variables de jeu d’essai
  var forfaitsResultat: DataFrame         = _
  var reglesGestion: DataFrame            = _
  var numTaxation: DataFrame              = _
  var NumTaxationInternational: DataFrame = _
  var spark: SparkSession                 = _

  // Portion de code executee avant chaque test
  before {
    // creation de la session
    spark = SparkSession.builder().appName("SparkApp").master("local").getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite
    // lecture des jeux de tests
    numTaxation = FonctionsUtiles.readFileComplet(
      spark = spark,
      schemaDescrptif = null,
      path = "src/test/resources/numerotation_taxation.csv",
      delimiter = ",",
      headerStatus = true
    )
    NumTaxationInternational = FonctionsUtiles.readFileComplet(
      spark = spark,
      schemaDescrptif = null,
      path = "src/test/resources/numerotation_taxation_international.csv",
      delimiter = ",",
      headerStatus = true
    )
    reglesGestion = FonctionsUtiles.readFileComplet(
      spark = spark,
      schemaDescrptif = null,
      path = "src/test/resources/reglesGestion4527692.csv",
      delimiter = "$",
      headerStatus = true
    )
    forfaitsResultat =
      FonctionsUtiles.readFileComplet(spark, null, "src/test/resources/forfaits4527692.csv", "$", headerStatus = true)

  }

  test("Verification si la proc Forfait ne retourne pas un dataframe vide") {
    // WHEN
    val data = procForfaits(spark, reglesGestion, numTaxation, NumTaxationInternational)
    // THEN
    assert(!data.head(1).isEmpty)
  }
  test("Verification si le nombre de colonnes attendu en fin de forfait est de 51") {
    // WHEN
    val data = procForfaits(spark, reglesGestion, numTaxation, NumTaxationInternational)
    // THEN
    assert(data.columns.length === 51)
  }

  test("Verification si la sortie de forfait correspond bien au jeu de test de sortie forfait") {
    // WHEN
    val data = procForfaits(spark, reglesGestion, numTaxation, NumTaxationInternational)
    // THEN
    assert(forfaitsResultat.except(data).count() === 0)
  }

}
