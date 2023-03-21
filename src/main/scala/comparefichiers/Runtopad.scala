package comparefichiers

import org.apache.spark.sql.{functions => f}
import stats.Run.logger
import utils.Utils

object Runtopad {
  // val logger:Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val repertoireBaseSpark = args(0)
    val fromKnime           = args(1)
    val fromSpark           = args(2)
    val base                = args(3)
    val table               = args(4)

    ////////////////////////////////////
    /// CONF : Creation spark Session //
    ////////////////////////////////////
    // Creation de la Session
    val spark = Utils.creerSparkSession(repertoireBaseSpark)
    Utils.CreerEtSetBasePardefaut(base)
    logger.info("Creation de la session Spark de l'application")

    // calcul des differences
    val rgid = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .load(fromKnime)
    // .where(f.col("TicketId")==="4500063")

    rgid.show(false)
    // rgid.where(f.col("TicketId")==="4483899") 4506340
    val sprgid = spark
      .table(s"$table")
      .selectExpr(
        "Code_UA",
        "Code_type_de_structure",
        "Etat",
        "DIR_hierarchique",
        "CI_hierarchique",
        "CB_hierarchique",
        "Bureau",
        "Bureau_2",
        "Bureau_court"
      )
    // .withColumn("CB_hierarchique",when(f.col("CB_hierarchique")==="",lit(null)))
    // .where(f.col("TicketId")==="4500063")
    sprgid.show(false)

    val ListDropped =
      List("Direction", "Date_deffet_de_letat", "Centre_des_Impots", "Libelle_court_2", "CB_hierarchique")
    calculerDifferencesDfPrintln(
      rgid.drop(ListDropped: _*).orderBy(f.asc("Code_UA")),
      sprgid.drop(ListDropped: _*).orderBy(f.asc("Code_UA")),
      differencesParColonne = false,
      ""
    )

    logger.info("Fin de la session")
    spark.stop()

  }
}
