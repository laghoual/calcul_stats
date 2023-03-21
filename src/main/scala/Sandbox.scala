import datawrap.core.Variables
import models.bronze.tip._
import org.apache.spark.sql.{DataFrame, functions => f}

object Sandbox extends App {

  // TODO: Dynamic partitioning for hive tables only works with SQL :-(
  // This will require quite the ugly workaround
  // https://stackoverflow.com/a/61042782

  /** Assignation des paramètres */
//  val dateManuelleFichiers = "2022-04-28" // ex: 2022-04-28 ou 20220428
  val dateManuelleFichiers = "2023-01-11" // ex: 2022-04-28 ou 20220428

  // val graph = ModelGraph(SagesExport, StatsExport)
  // graph.export.exportToPng("graph")

  val variables = new Variables(Map("jour" -> dateManuelleFichiers))

  // test manuel du Forfait après le rename des Items
  val df = Forfaits.run(variables, refreshSources = true)
  df.select("Appel_vers").distinct().show(false)
  df.printSchema()

  // test manuel des règles de gestions après la modif de l'ensemble des règles
  // val df2 = ReglesGestion.run(variables, refreshSources = true)
  // df2
  // .select(
  // "Attente_avant_mise_en_relation",
  // "Appels_dissuades",
  // "Mise_en_relation",
  // "Appels_non_decroches",
  // "Appels_decroches",
  // "Aboutissement_SVI",
  // "Horaire_ouverture"
  // )
  // .show(false)

  // df2.select("Horaire_ouverture").distinct().show(false)

  // df.withColumn(
  // "Appel_vers",
  // f.when(f.col("Appel_vers") === "N° courts (services)", f.lit("Numéros d'urgence"))
  // .when(
  // (f.col("Appel_vers") === "N° courts (autre)") || (f.col("Appel_vers") === "N° courts (urgence)"),
  // f.lit("Numéros interne")
  // )
  // ).select("Appel_vers").distinct().show(false)

  // Checksum pour 2022-04-28: 1cc07e1283b6bb02e5ee161158f0adf6
  // Checksum pour 2023-01-11: d4f7f3d8c7cf7da6d8b433dc1b4735d6
  // 66033 lignes
  // StatsExport.runAndExport(variables, refreshSources = false)

  // Checksum pour 2022-04-28: e5e63047c71dc2a29a72ff4cebec0138
  // Checksum pour 2022-04-28: e5e63047c71dc2a29a72ff4cebec0138
  // Checksum pour 2023-01-11: d5a8b2690f4afe7a1a20e493fc07b7a0
  // SagesExport.runAndExport(variables, refreshSources = false)

}
