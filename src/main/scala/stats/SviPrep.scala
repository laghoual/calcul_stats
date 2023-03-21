package stats

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SviPrep {

  // creation de la classe permettant de gérer les logs
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  // Vérification des colonnes
  def controleColonnesDf(df: DataFrame, listColumns: List[String]): Boolean = {
    listColumns.foreach(a =>
      if (!df.columns.contains(a)) {
        return false
      }
    )
    true
  }

  def procSVI(spark: SparkSession, sviStat: DataFrame, sviNode: DataFrame): DataFrame = {

    logger.info("Traitement SVI")

    // ****************************************************** NODENAME *******************************************

    // Traitement de controle SVI
    val listColumns = List("id", "duration (ms)", "bloc id", "bloc name", "timestamp", "event", "createdate")

    if (!controleColonnesDf(sviNode, listColumns)) {
      logger.info("Le fichier SVINODE n'est pas conforme")
      spark.stop()
    }

    // *********************************************************************************************************

    val df_sviNode = sviNode
      .select(
        "id",
        "duration (ms)",
        "bloc id",
        "bloc name",
        "timestamp",
        "event",
        "createdate"
      )
      .withColumn(
        "date",
        f.concat_ws(".", f.from_unixtime(f.col("timestamp"), "yyyy-MM-dd HH:mm:ss"), f.substring(f.col("timestamp"), -3, 3))
      )
      .withColumn("bloc duration", f.col("duration (ms)") / 1000)

    val win = Window.partitionBy("id").orderBy(f.desc("timestamp"))

    var sviNode_grouped = df_sviNode.withColumn("rank", f.row_number().over(win)).where("rank == 1")

    sviNode_grouped = sviNode_grouped.select(
      "id",
      "bloc duration",
      "bloc name"
    )

    // *****************************************************************************************************

    // 2 eme partie du schèma ( 2 eme Partie du traitement du fichier SVI )

    val df_sviNode2 = sviNode
      .filter(f.col("bloc id").contains("transfer_"))
      .withColumn(
        "date",
        f.concat_ws(".", f.from_unixtime(f.col("timestamp"), "yyyy-MM-dd HH:mm:ss"), f.substring(f.col("timestamp"), -3, 3))
      )

    // Supprimer les doublons
    val win_2            = Window.partitionBy("id").orderBy(f.desc("timestamp"))
    var sviNode_grouped2 = df_sviNode2.withColumn("rank", f.row_number().over(win_2)).where("rank == 1")

    sviNode_grouped2 = sviNode_grouped2.withColumnRenamed("bloc name", "Motif appel")

    sviNode_grouped2 = sviNode_grouped2.select(
      "id",
      "Motif appel",
      "bloc id"
    )

    // **********************************************************************************************************

    // Jointure  entre les 2 fichiers SVINODE traités
    val join_sviNode = sviNode_grouped.join(sviNode_grouped2, Seq("id"), "left")

    // ****************************************************************************************************************

    // Ajout du 2 eme fichier SVI STAT

    // Traitement de controle SVI STAT
    val listColumnsSvi =
      List("id", "caller", "called", "tenantname", "treename", "start", "duration", "endcause", "transferredto")

    if (!controleColonnesDf(sviStat, listColumnsSvi)) {
      logger.info("Le fichier SVISTAT n'est pas conforme, il manque : ")
      spark.stop()
    }

    // *********************************************************************************************************

    // Préparation du fichier SVI STAT
    val sviStatFormated = sviStat
      .selectExpr(
        "id",
        "caller",
        "called",
        "tenantname",
        "treename",
        "start",
        "duration",
        "endcause",
        "transferredto"
      )
      .withColumn("started_datetime", f.col("start").cast(TimestampType))

    // pour la recette cette exclusion de ligne est demandée
    // val recetteDataExcludedSrc = FonctionsUtiles.readFile(spark,true,"c:/users/ymikponhoue01/documents/spark_env/toip/donnees/recette/ecart_date.csv",",","CSV")
    // val sviStatClean = sviStatFormated.join(recetteDataExcludedSrc,Seq("id","duration"),"left")
    // val sviStatCleaned = sviStatClean.withColumn("started_datetime",when(f.col("new_date")==="?",lit("?")).otherwise(f.col("started_datetime"))).drop("new_date")

    // Jointure finale entre SVINODE et SVISTAT
    var join_SVI = join_sviNode.join(sviStatFormated, Seq("id"), "inner")

    join_SVI = join_SVI
      .withColumn("SAGES (bloc)", f.regexp_extract(join_SVI("bloc name"), "\\W(\\d\\w{5,7})", 1))
      .withColumn("temp_caller", f.regexp_extract(join_SVI("caller"), "sip:(.*)\\@.*", 1))
      .withColumn("temp_caller", f.rtrim(f.col("temp_caller")))

    join_SVI

  }

}
