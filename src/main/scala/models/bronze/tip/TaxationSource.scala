package models.bronze.tip

import conf.AppConf
import datawrap.core.{Materialization, Variables}
import datawrap.external.input.CsvSparkImport
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

object TaxationSource
    extends CsvSparkImport(
      header = true,
      delimiter = ","
//      ,
//      extraOptions = Map("nullValue" -> "NULL")
    ) {

  override val materialization: Materialization = Materialization.EPHEMERAL

  override val declaredVariables: Set[String] = Set("jour")

  val schemaString: String =
    """
      |InitialStartDateTime  string,
      |TicketId              string,
      |CalledNumber          string comment "#{regexify '[0-9]{7,9}'}",
      |CallingNumber         string comment "#{regexify '[0-9]{7,9}'}",
      |CallType              string comment "#{numerify '#'}",
      |EffectiveCallDuration string,
      |InitialDialledNumber  string comment "#{regexify '[0-9]{7,9}'}",
      |StartDateTime         string,
      |InternFacilities      string,
      |ChargedNumber         string comment "#{regexify '[0-9]{7,9}'}",
      |CostCenter            string,
      |WaitingDuration       string
      |""".stripMargin

  override val inputSchema: Option[StructType] = None

  override val outputSchema: Option[StructType] = Some(StructType.fromDDL(schemaString))

  override def getInputPath(variables: Variables): String =
    s"${AppConf.repertoireSourceDesFichiers}/taxation/all/ResultatDGFIP_Taxation_DailyStats_${variables("jour")
        .replace("-", "")}.csv"

  override def compute(implicit variables: Variables): DataFrame = {

    /** Le csv contient beaucoup de colonnes dont on ne veut pas. On applique la stratégie suivante:
      *   - On n'applique pas de schéma à l'entrée pour récupérer toutes les colonnes (inputSchema = None)
      *   - On applique un `select` pour ne garder que les colonnes qui nous intéressent
      */
    val df: DataFrame                = super.compute(variables)
    val ColumnsToKeep: Array[Column] = outputSchema.get.fields.map(field => df(field.name))
    df.select(ColumnsToKeep: _*)
  }
}
