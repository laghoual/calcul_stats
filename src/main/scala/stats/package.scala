import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import stats.ConstantesColonnes.COLSFORFAITFINAL
import stats.Run.logger
import utils.FonctionsUtiles

package object stats {

  /** Trim le contenu de chaque colonne d'un dataFrame et met à null les chaines vides
    *
    * @param df
    *   : DataFrame à nettoyer
    * @return
    *   Dataframe avec trim sur chaque colonne et chaines vides en null
    */
  def nettoyerDfTrim(df: DataFrame): DataFrame = {

    val resultDf = df.columns.foldLeft(df)((acc, colname) => {
      acc.withColumn(colname, f.trim(f.col(colname)))
    })

    resultDf
  }

  def filterByLen(topadWithColsSelected: DataFrame, longueur_code_UA: Int): DataFrame = {
    topadWithColsSelected.where(
      f.col("longueur_code_UA") === longueur_code_UA && f.col("CB_hierarchique").isNull && f
        .col(
          "CI_hierarchique"
        )
        .isNotNull && f.col("DIR_hierarchique").isNotNull
    )
  }

  def creerDfAvecPosition(fichierSrc: DataFrame, positionsDf: DataFrame): DataFrame = {
    // selection des colonnes et suppressions des lignes contenant des valeurs nulles
    val positionsSelected = positionsDf.selectExpr("nom as Nom", "de", "long", "a")
    // Nettoyer le Df contenant les différentes positions et le nom des colonnes
    val positionsControlled = FonctionsUtiles.NettoyerDfPositionnel(positionsSelected)

    // Créer une liste de tuples à partir du df positionnel
    val listeTuple = FonctionsUtiles.creerListeTuplesAvecDf(positionsControlled)

    // renommage de la colonne
    val fichier = fichierSrc.selectExpr("_c0 as content")

    // creation du Df contenant tous les enregistrements à partir du df positionnel et du fichier de donnée
    val allEnreg     = FonctionsUtiles.creerDfAvecListesTupleCtl(listeTuple, fichier)
    val tableFiltree = allEnreg

    // renommage des colonnes avant écriture en table
    // les caractères "-" n'étant pas acceptés il est important de les remplacer par _
    val tableFinale = tableFiltree
      .drop("content")

    tableFinale

  }

  def readFileExcel(spark: SparkSession, chemin: String, nomFeuille: String, header: Boolean): DataFrame = {
    val df = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("dataAddress", nomFeuille)                // Required
      .option("useHeader", "true")                      // Required
      .option("treatEmptyValuesAsNulls", "false")       // Optional, default: true
      .option("inferSchema", "false")                   // Optional, default: false
      .option("addColorColumns", "true")                // Optional, default: false
      .option("startColumn", 0)                         // Optional, default: 0
      .option("endColumn", 99)                          // Optional, default: Int.MaxValue
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option(
        "maxRowsInMemory",
        20
      ) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option(
        "excerptSize",
        10
      ) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      // .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load(chemin)
    df
  }

  def unionLeftDFwithRightDF(left_df: DataFrame, right_df: DataFrame): DataFrame = {
    left_df.union(right_df.selectExpr(left_df.columns: _*))
  }

  // Vérification des colonnes
  def controleColonnesDf(df: DataFrame, listColumns: List[String]): Boolean = {
    listColumns.foreach(a =>
      if (!df.columns.contains(a)) {
        return false
      }
    )
    true
  }
  // fonction
  def gestionCallsSpeciaux(
      dfGestionSrc: DataFrame,
      numerotationTaxation: DataFrame,
      longueurChamp: Int,
      ContenuAppelVers: String
  ): DataFrame = {
    var dfGestion = dfGestionSrc.withColumn("new", f.substring(f.col("CalledNumber"), 0, longueurChamp))

    // Jointure entre les données récupérer par le calcul des règles de gestion et le fichier qui contient les indicatifs
    dfGestion = dfGestion
      .join(numerotationTaxation, f.col("new") === f.col("Debut") && f.col("len") === f.col("Longueur"), "leftouter")
      .withColumn(
        "Appel_facturable",
        f.when(f.col("Debut").isNotNull, "Forfait")
          .otherwise("Hors forfait")
      )

    val dfGestionReturned = dfGestion
      .withColumn("Appel_vers", f.lit(ContenuAppelVers))
      .selectExpr(COLSFORFAITFINAL: _*)
    dfGestionReturned.selectExpr(COLSFORFAITFINAL: _*)

  }
  def gestionCourtSpeciaux(
      reglesGestionLen8: DataFrame,
      appel_facturable: String,
      Appel_vers: String,
      Pays: String,
      Indicatif: String
  ): DataFrame = {
    reglesGestionLen8
      .withColumn("Appel_facturable", f.lit(appel_facturable))
      .withColumn("Appel_vers", f.lit(Appel_vers))
      // .withColumn("Pays",f.lit(Pays))
      // .withColumn("Indicatif",f.lit(Indicatif))
      .drop("len")
      .selectExpr(COLSFORFAITFINAL: _*)
  }

  def repeatRows(df: DataFrame, numRepeats: Int): DataFrame = {
    (1 until numRepeats).foldLeft(df)((growingDF, _) => growingDF.union(df))
  }

  def conversionDayofWeek(df: DataFrame, colDf: Column, colnameString: String): DataFrame = {
    df.withColumn(colnameString, f.regexp_replace(colDf, "Monday", "lundi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Tuesday", "mardi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Wednesday", "mercredi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Thursday", "jeudi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Friday", "vendredi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Saturday", "samedi"))
      .withColumn(colnameString, f.regexp_replace(colDf, "Sunday", "dimanche"))
  }

  def creationColsDate(df: DataFrame): DataFrame = {
    val forfaitsUsagersTopadSagesDateModifs = df
      .withColumn("Minutes", f.minute(f.col("InitialStartDateTime")))
      .withColumn("Hour", f.hour(f.col("InitialStartDateTime")))
      .withColumn("Date", f.to_date(f.col("InitialStartDateTime")))
      .withColumn("dayofweek2", f.dayofweek(f.col("InitialStartDateTime")))
      .withColumn("dayofweek", f.date_format(f.col("InitialStartDateTime"), "EEEE"))

    val forfaitsUsagersTopadSagesQuarter = forfaitsUsagersTopadSagesDateModifs
      .withColumn(
        "quarter",
        f.when(f.col("Minutes") < 15, f.lit("00-15"))
          .when(f.col("Minutes") < 30, f.lit("15-30"))
          .when(f.col("Minutes") < 45, f.lit("30-45"))
          .otherwise("45-00")
      )
      .withColumn("hour_45", f.when(f.col("quarter") === "45-00", f.col("Hour") + 1).otherwise(f.col("Hour")))
      .withColumn("hour_evol", f.when(f.col("hour_45") === "24", f.lit("00")).otherwise(f.col("hour_45")))

    val forfaitsUsagersTopadSagesLib = forfaitsUsagersTopadSagesQuarter
      .withColumn("Lib_hour_part1", f.concat(f.col("Hour"), f.lit(":"), f.substring(f.col("quarter"), 0, 2)))
      .withColumn("Lib_hour_part2", f.concat(f.col("hour_evol"), f.lit(":"), f.substring(f.col("quarter"), 4, 2)))
      .withColumn("Lib_hour", f.concat(f.col("Lib_hour_part1"), f.lit(" - "), f.col("Lib_hour_part2")))
      .drop("Lib_hour_part1", "Lib_hour_part2", "dayofweek2", "hour_45", "hour_evol")

    val forfaitsUsagersTopadFrenchDays =
      conversionDayofWeek(forfaitsUsagersTopadSagesLib, f.col("dayofweek"), "dayofweek")

    val forfaitsUsagersTopadExport = forfaitsUsagersTopadFrenchDays
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), " ", "T"))
      .withColumn("InitialStartDateTime", f.regexp_replace(f.col("InitialStartDateTime"), ".000", ""))

    forfaitsUsagersTopadExport
  }

}
