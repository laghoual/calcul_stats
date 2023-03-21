package stats

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class PackageTest extends AnyFunSuite with BeforeAndAfter {

  // Given
  // Creation des variables de jeu d’essai
  var dataRaw: DataFrame                   = _
  var positionRaw: DataFrame               = _
  var jeudetestResultat: DataFrame         = _
  var jeudetestResultatPosition: DataFrame = _
  var initialDf: DataFrame                 = _
  var positionRawNettoyer: DataFrame       = _
  var resultatControleColonne: DataFrame   = _
  // Portion de code executee avant chaque test
  before {

    val spark = SparkSession.builder().appName("SparkApp").master("local[*]").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite

    // annexe
    dataRaw = List("yo yaa  yi88888888888").toDF("_c0")

    // jeu de test en entree
    positionRawNettoyer =
      Seq(("nom1 ", "1", "2"), (" nom2", "4", "3"), ("nom3", " 8", "2 ")).toDF("nom", "de", "longueur")

    // jeu de test en entree
    positionRaw = Seq(("nom1", "1", "2", "1"), ("nom2", "4", "3", "4")).toDF("nom", "de", "long", "a")

    // ce jeu de test est le resultat auquel on s'attend apres le test
    jeudetestResultat = Seq(("nom1", "1", "2"), ("nom2", "4", "3"), ("nom3", "8", "2")).toDF("nom", "de", "longueur")

    // ce jeu de test est le resultat auquel on s'attend apres le test
    jeudetestResultatPosition = Seq(("yo", "yaa")).toDF("nom1", "nom2")

    // test
    initialDf = Seq((4477743, "2022-04-27 00:00:35.000")).toDF("id", "InitialStartDateTime")
    resultatControleColonne = Seq(
      ("4477743", "2022-04-27T00:00:35", 0, 0, "2022-04-27", "mercredi", "00-15", "0:00 - 0:15")
    ).toDF("id", "InitialStartDateTime", "Minutes", "Hour", "Date", "dayofweek", "quarter", "Lib_hour")

  }

  test("verification de l'egalite des dataframes entree et sortie de nettoyerDfTrim") {
    // WHEN
    val data = nettoyerDfTrim(positionRawNettoyer)
    // THEN
    assert(jeudetestResultat.except(data).count() === 0)
  }

  test("verification de l'egalite des dataframes entree et sortie de creerDfAvecPosition") {
    // WHEN
    val data = creerDfAvecPosition(dataRaw, positionRaw)
    // THEN
    assert(jeudetestResultatPosition.except(data).count() === 0)
  }

  test("verification que les DataFrames en sortie et en entrée sont égaux sur unionLeftDFwithRightDF") {
    // WHEN
    val data = unionLeftDFwithRightDF(jeudetestResultat, positionRawNettoyer)
    // THEN
    assert(data.count() === 6)
    assert(data.schema.fields.map(_.name).toList === List("nom", "de", "longueur"))
  }

  test("verification que la sortie du controle colonne est conforme dans creationColsDate") {
    // WHEN
    val data = creationColsDate(initialDf)
    // THEN
    assert(resultatControleColonne.except(data).count() === 0)
  }

  test("verification que le nombre de lignes est bien celui attendu sur repeatRows") {
    // WHEN
    val nbLignes = 5
    val data     = repeatRows(dataRaw, nbLignes)
    // THEN
    assert(data.count === nbLignes)
  }

}
