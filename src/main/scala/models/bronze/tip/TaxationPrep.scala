package models.bronze.tip

import datawrap.core.{Materialization, SparkModel, Variables}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, functions => f}

object TaxationPrep extends SparkModel {

  override val declaredVariables: Set[String] = Set()

  override val inputs: Set[SparkModel] = Set(TaxationSource)

  override val materialization: Materialization = Materialization.EPHEMERAL

  val schemaString: String =
    """
      |InitialStartDateTime  string,
      |TicketId              string,
      |CalledNumber          string,
      |CallingNumber         string,
      |CallType              string,
      |EffectiveCallDuration int,
      |StartDateTime         string,
      |InternFacilities      string,
      |ChargedNumber         string,
      |CostCenter            string,
      |WaitingDuration       string
      |""".stripMargin

  override val outputSchema: Option[StructType] = Some(StructType.fromDDL(schemaString))

  override protected def compute(implicit variables: Variables): DataFrame = {
    // Traitement du fichier Taxation
    var df_taxation = TaxationSource.select(
      f.date_format(f.col("InitialStartDateTime"), "yyyy-MM-dd HH:mm:ss.SSS").as("InitialStartDateTime"),
      f.col("TicketId"),
      f.col("CalledNumber"),
      f.col("CallingNumber"),
      f.col("CallType"),
      f.col("EffectiveCallDuration").cast("Integer"),
      f.col("InitialDialledNumber"),
      f.date_format(f.col("StartDateTime"), "yyyy-MM-dd HH:mm:ss.SSS").as("StartDateTime"),
      f.col("InternFacilities"),
      f.col("ChargedNumber"),
      f.col("CostCenter"),
      f.col("WaitingDuration")
    )

    /** DTK-1058: La colonne ChargedNumber vaut parfois "" au lieu de NULL. Cette transformation corrige le tir.
      */
    df_taxation = df_taxation.withColumn(
      "ChargedNumber",
      f.when(f.col("ChargedNumber") === f.lit(""), f.lit(null)).otherwise(f.col("ChargedNumber"))
    )

    /** DTK-1058: On supprime toutes les lignes du fichier Taxation dont l’un des 4 numéros qui suit est non-null, fait
      * 8 chiffres et commence par 1.
      */
    df_taxation = df_taxation
      .where(f.not(PredicatLeNumeroFaitHuitChiffresEtCommenceParUn("CalledNumber")))
      .where(f.not(PredicatLeNumeroFaitHuitChiffresEtCommenceParUn("ChargedNumber")))
      .where(f.not(PredicatLeNumeroFaitHuitChiffresEtCommenceParUn("CallingNumber")))
      .where(
        f.not(PredicatLeNumeroFaitHuitChiffresEtCommenceParUn("InitialDialledNumber"))
      )

    df_taxation
  }

  /** DTK-1058: Supprimer toutes les lignes du fichier Taxation dont l’un des 4 numéros qui suit est non-null, fait 8
    * chiffres et commence par 1:
    *
    *   - CalledNumber
    *   - ChargedNumber
    *   - CallingNumber
    *   - InitialDialledNumber
    *
    * @param colName:
    *   nom de la colonne à traiter
    * @return
    */
  def PredicatLeNumeroFaitHuitChiffresEtCommenceParUn(colName: String): Column = {
    val col = f.col(colName)
    f.when(col.isNull, f.lit(false))
      .otherwise(
        (f.length(col) === f.lit(8)) && (f.substring(col, 0, 1) === f.lit("1"))
      )
  }

}
