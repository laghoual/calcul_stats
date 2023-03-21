package models.bronze.tip

import conf.AppConf
import datawrap.core.{Materialization, Variables}
import datawrap.external.input.CsvSparkImport
import org.apache.spark.sql.types.StructType

object UsagersSource
    extends CsvSparkImport(
      header = true,
      delimiter = ","
      //      , extraOptions = Map("nullValue" -> "NULL")
    ) {

  override val inputSchema: Option[StructType] = None

  override val materialization: Materialization = Materialization.EPHEMERAL

  override val declaredVariables: Set[String] = Set("jour")

  override def getInputPath(variables: Variables): String =
    s"${AppConf.repertoireSourceDesFichiers}/usagers/all/ResultatDGFIP_Usagers_${variables("jour").replace("-", "")}.csv"

}
