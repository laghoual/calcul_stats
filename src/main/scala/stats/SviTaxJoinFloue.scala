package stats

//1
import org.apache.log4j.Logger
import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SviTaxJoinFloue {

  // creation de la classe permettant de gérer les logs
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def procJointureFloue(spark: SparkSession, svi: DataFrame, taxation: DataFrame): DataFrame = {
    logger.info("SviTaxJoinFloue")
    // Filter les champs du fichier taxation
    val taxationSelected = taxation.selectExpr(
      "InitialStartDateTime",
      "StartDateTime",
      "TicketId",
      "CalledNumber",
      "InternFacilities",
      "CallType",
      "ChargedNumber",
      "EffectiveCallDuration",
      "CostCenter",
      "CallingNumber",
      "WaitingDuration"
    )

    // filter les champs svi
    val sviSelected = svi.selectExpr(
      "started_datetime",
      "temp_caller",
      "endcause"
    )

    // Jointure entre les données taxation et les données SVI
    val taxationSvi = taxationSelected.join(sviSelected, f.col("CalledNumber") === f.col("temp_caller"), "inner")

    // Ne garder que les données dont l'écart entre les dates < 4 secondes
    // Pour matcher avec la recette modifications manuelles à effectuer
    /*val taxationSviFilteredByDate = taxationSvi.withColumn("date_diff", f.abs(f.to_timestamp(f.col("started_datetime"),"yyyy-MM-dd HH:mm:ss").cast(LongType) - f.to_timestamp(f.col("InitialStartDateTime"),"yyyy-MM-dd HH:mm:ss").cast(LongType)))
    .where( ((f.abs(f.col("date_diff"))<=4 and f.abs(f.col("date_diff"))>=0))
      || (f.col("TicketId").isin("4507397","4505252", "4525913", "4544523", "4485093", "4506052", "4505559", "4499770", "4526073", "4526049", "4526034")
      and f.col("started_datetime").isin("2022-04-27 10:46:37.899", "2022-04-27 10:46:37.899", "2022-04-27 13:31:31.172", "2022-04-27 15:15:41.495", "2022-04-27 08:15:42.131", "2022-04-27 10:37:31.351", "2022-04-27 10:53:24.123", "2022-04-27 09:57:39.335", "2022-04-27 13:31:16.876", "2022-04-27 13:31:16.876", "2022-04-27 13:31:16.876")
      )
    )//.drop("date_diff") */

    val taxationSviFilteredByDate = taxationSvi
      .withColumn(
        "date_diff",
        f.abs(
          f.to_timestamp(f.col("started_datetime"), "yyyy-MM-dd HH:mm:ss")
            .cast(LongType) - f.to_timestamp(f.col("InitialStartDateTime"), "yyyy-MM-dd HH:mm:ss").cast(LongType)
        )
      )
      .where(f.abs(f.col("date_diff")) <= 4 and f.abs(f.col("date_diff")) >= 0) // .drop("date_diff")

    // logger.info("join flouee"+taxationSviFilteredByDate.count())

    // Supprimer les champs pour les récupérer après la jointure de l'autre dataframe
    val taxationSviFilteredByDateSelected = taxationSviFilteredByDate.drop(
      "WaitingDuration",
      "Callednumber",
      "Calltype",
      "StartDateTime",
      "Initialstartdatetime",
      "Chargednumber",
      "Internfacilities",
      "EffectiveCallDuration",
      "CostCenter",
      "CallingNumber"
    )

    // debug suivi
    // val filtered = taxationSviFilteredByDateSelected.count
    // logger.info(s"filtered before join $filtered")

    // 2 eme jointure avec les données taxation
    val taxationWithSvi = taxationSelected.join(taxationSviFilteredByDateSelected, Seq("TicketId"), "leftouter")

    // debug suivi
    // val countJoinsviTaxationSecJoin = taxationWithSvi.count
    // logger.info(s"deuxieme jointure svi taxation $countJoinsviTaxationSecJoin")

    // Récupérer une deuxième version des données svi
    val sviRename = svi
      .withColumnRenamed("started_datetime", "date")
      .withColumnRenamed("temp_caller", "temp")
      .withColumnRenamed("endcause", "end")

    // Jointure finale avec les données svi
    val sviTaxationCleaned = taxationWithSvi.join(
      sviRename,
      f.col("CalledNumber") === f.col("temp") && f.col("started_datetime") === f.col("date"),
      "leftouter"
    )
    // .drop(join("Callednumber"), "join.Calltype", "join.Initialstartdatetime", "join.Chargednumber", "join.Internfacilities")
    sviTaxationCleaned

  }

}
