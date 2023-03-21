package stats

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import stats.Croisements.procCroisements
import utils.{FonctionsUtiles, Utils}

class CroisementsTest extends AnyFunSuite with BeforeAndAfter {
  // Given
  // Creation des variables
  var usagersTopad: DataFrame       = _
  var forfaits: DataFrame           = _
  var topad: DataFrame              = _
  var colonnesFinales: List[String] = _
  var spark: SparkSession           = _

  // Portion de code executee avant chaque test
  before {
    // creation de la session
    spark = SparkSession.builder().appName("SparkApp").master("local[*]").enableHiveSupport().getOrCreate()

    // set de la base
    Utils.CreerEtSetBasePardefaut("test")
    spark.sparkContext.setLogLevel("WARN") // diminuer la verbosite

    // lecture des jeux de tests
    usagersTopad =
      FonctionsUtiles.readFileComplet(spark, null, "src/test/resources/usagertopad.csv", "$", headerStatus = true)
    topad =
      FonctionsUtiles.readFileComplet(spark, null, "src/test/resources/topad_transformed.csv", "$", headerStatus = true)
    forfaits =
      FonctionsUtiles.readFileComplet(spark, null, "src/test/resources/forfaits4527692.csv", "$", headerStatus = true)
    colonnesFinales = List(
      "InitialStartDateTime",
      "CalledNumber",
      "CallingNumber",
      "ChargedNumber",
      "transferredto",
      "Motif_appel",
      "Type_appel",
      "CalledNumber_externe_interne",
      "Origine",
      "Origine_appel_entrant",
      "Appels_emis",
      "Appels_recus",
      "Appels_reiteres",
      "Mises_en_relation",
      "Appels_SVI",
      "Appels_dissuades",
      "Appels_raccroches",
      "Appels_SVI_non_reconnu",
      "Appels_directs_internes",
      "Appels_directs_externes",
      "Appels_aboutis",
      "Appels_decroches",
      "Appels_non_decroches",
      "Appels_transferes",
      "Appels_renvoyes",
      "Attente_avant_mise_en_relation",
      "Duree_de_navigation_SVI",
      "Duree_de_communication",
      "postes_logges",
      "Aboutissement_SVI",
      "Appel_facturable",
      "Appel_vers",
      "Pays",
      "SAGES",
      "Day_of_week",
      "Hour",
      "Date",
      "Service",
      "Quarter_of_hour",
      "Lib_hour"
    )
  }
  test("Verification si le nombre de colonnes attendu en fin de la proc Croisement est de 40") {
    // WHEN
    procCroisements(usagersTopad, forfaits, topad)
    val data = spark.table("stats")
    // THEN
    assert(data.columns.length === 40)
  }

  test("Verification si les noms de colonnes sont corrects") {
    // WHEN
    procCroisements(usagersTopad, forfaits, topad)
    val data = spark.table("stats")
    // THEN
    assert(data.schema.fields.map(_.name).toList === colonnesFinales)
  }
}
