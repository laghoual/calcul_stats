package models.bronze.tip_topad

import datawrap.core.{Materialization, SparkModel, Variables}
import org.apache.spark.sql.{DataFrame, functions => f}
import stats.ConstantesColonnes.COLSTOPAD
import stats.{creerDfAvecPosition, nettoyerDfTrim}

object Topad extends SparkModel {

  override val declaredVariables: Set[String] = Set()

  override val inputs: Set[SparkModel] = Set(TopadSource, TopadConfSource)

  override val materialization: Materialization = Materialization.EPHEMERAL

  override protected def compute(implicit variables: Variables): DataFrame = {
    val topadWithCols = creerDfAvecPosition(TopadSource, TopadConfSource)

    val topadColsTrimed = nettoyerDfTrim(topadWithCols)
    // selection des bonnes colonnes et suppression de la premiere ligne
    val topadWithColsSelected =
      topadColsTrimed.where(!(f.col("Code_type_de_structure") === "CTURE")).selectExpr(COLSTOPAD: _*)
    // ajout de la date
    val topadWithDate = topadWithColsSelected.withColumn("Today", f.current_date())

    // regroupement par ua et date de validité
    val topadGroupedByUa =
      topadWithDate.groupBy("Code_UA").agg(f.max(f.col("Date_limite_de_validite")).as("Date_limite_de_validite"))
    // jointure avec topad daté
    val topadwithGroupedUa = topadWithDate.join(topadGroupedByUa, Seq("Code_UA", "Date_limite_de_validite"))

    // elimination des ua invalides
    val topadValidated = topadwithGroupedUa.where(
      f.col("Date_limite_de_validite") === "99999999"
        || f.col("Date_limite_de_validite") > f.col("Today")
    )

    // group by sur ua
    val topadGroupedByUaFirstCols = topadValidated
      .groupBy("Code_UA")
      .agg(
        f.first(f.col("Date_limite_de_validite")).as("Date_limite_de_validite"),
        f.first(f.col("Code_type_de_structure")).as("Code_type_de_structure"),
        f.first(f.col("Etat")).as("Etat"),
        f.first(f.col("Date_deffet_de_letat")).as("Date_deffet_de_letat"),
        f.first(f.col("DIR_hierarchique")).as("DIR_hierarchique"),
        f.first(f.col("CI_hierarchique")).as("CI_hierarchique"),
        f.first(f.col("CB_hierarchique")).as("CB_hierarchique"),
        f.first(f.col("Libelle_court_1")).as("Libelle_court_1"),
        f.first(f.col("Libelle_court_2")).as("Libelle_court_2"),
        f.first(f.col("Libelle_long_2")).as("Libelle_long_2"),
        f.first(f.col("Libelle_long_1")).as("Libelle_long_1")
      )

    // convertir les dates
    val topadDateConverted = topadGroupedByUaFirstCols
      .withColumn("Date_deffet_de_letat", f.to_date(f.col("Date_deffet_de_letat"), "yyyyMMdd"))
      .withColumn("Date_limite_de_validite", f.to_date(f.col("Date_limite_de_validite"), "yyyyMMdd"))

    val topadTrimed = topadDateConverted
      .withColumn("code_UA", f.trim(f.col("code_UA")))
      .withColumn("CI_hierarchique", f.trim(f.col("CI_hierarchique")))
      .withColumn("CB_hierarchique", f.trim(f.col("CB_hierarchique")))
      .withColumn("DIR_hierarchique", f.trim(f.col("DIR_hierarchique")))

    val topadWithLongUA = topadTrimed.withColumn("longueur_code_UA", f.length(f.col("code_UA")))

    // filter les données topad selon la longueur du code UA
    // longueur 7
    val topadLen7 = topadWithLongUA.where(
      f.col("longueur_code_UA") === 7 && f.col("CB_hierarchique") === "" && !(f.col("CI_hierarchique") === "") && !(f
        .col(
          "DIR_hierarchique"
        ) === "")
    )

    // longueur 5
    val topadLen5 = topadWithLongUA
      .where(
        f.col("longueur_code_UA") === 5 && (f.col("CB_hierarchique") === "") && (f.col("CI_hierarchique") === "") && !(f
          .col(
            "DIR_hierarchique"
          ) === "")
      )
      .select("Code_UA", "Libelle_long_1")
      .withColumnRenamed("Libelle_long_1", "Centre_des_Impots")
      .withColumnRenamed("Code_UA", "Code_UA_2")
    // longueur 3
    val topadLen3 = topadWithLongUA
      .where(
        f.col("longueur_code_UA") === 3 && (f.col("CB_hierarchique") === "") && (f.col("CI_hierarchique") === "") && (f
          .col(
            "DIR_hierarchique"
          ) === "")
      )
      .select("Code_UA", "Libelle_long_1")
      .withColumnRenamed("Libelle_long_1", "Direction")
      .withColumnRenamed("Code_UA", "Code_UA_3")

    // Jointure des différents subset
    val topadLen7Len5     = topadLen7.join(topadLen5, f.col("CI_hierarchique") === f.col("Code_UA_2"), "inner")
    val topadLen7Len5Len3 = topadLen7Len5.join(topadLen3, f.col("DIR_hierarchique") === f.col("code_UA_3"), "inner")

    //
    val topad = topadLen7Len5Len3
      .selectExpr(COLSTOPAD: _*)
      .withColumnRenamed("Libelle_long_1", "Bureau")
      .withColumnRenamed("Libelle_long_2", "Bureau_2")
      .withColumnRenamed("Libelle_court_1", "Bureau_court")
      .drop("Date_limite_de_validite")

    topad
  }

}
