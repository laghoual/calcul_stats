package controlefichiers
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class GestionFichiersTest extends AnyFunSuite {

  val DOSSIER_TEST_RESOURCES = "src/test/resources/controlefichiers/GestionFichiersTest"

  test("Verification si le Dataframe en sortie de procReglesGestion n'est pas vide") {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
//    val result: DataFrame = retrouverInfoFichiers(spark, this.DOSSIER_TEST_RESOURCES + "/taxation/all", "20220428")

//    result.show(truncate = false)
    // WHEN
    // THEN

  }

}
