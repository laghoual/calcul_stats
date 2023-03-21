package models.bronze.tip

import conf.AppConf
import datawrap.core.{Materialization, Variables}
import datawrap.external.input.CsvSparkImport
import org.apache.spark.sql.types.StructType

object SviNodeSource
    extends CsvSparkImport(
      header = true,
      delimiter = ","
    ) {

  private val schemaString: String =
    """
      |id              string    COMMENT 'Id appel SVI',
      |event           string    COMMENT '[ENTER_NODE, INCOMING, DISCONNECT]',
      |timestamp       double    COMMENT 'Date heure début bloc SVI',
      |createdate      timestamp COMMENT 'Date heure début bloc SVI',
      |`duration (ms)` int       COMMENT 'Durée du bloc SVI',
      |`bloc id`       string    COMMENT 'Id du bloc',
      |`bloc name`     string    COMMENT 'Texte du bloc'
      |""".stripMargin

  override val inputSchema: Option[StructType] = Some(StructType.fromDDL(schemaString))

  override val materialization: Materialization = Materialization.EPHEMERAL

  override val declaredVariables: Set[String] = Set("jour")

  override def getInputPath(variables: Variables): String =
    s"${AppConf.repertoireSourceDesFichiers}/svinode/all/ResultatDGFIP_NodeNames_${variables("jour").replace("-", "")}.csv"

}
