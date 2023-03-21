package models.bronze.tip

import conf.AppConf
import datawrap.core.{Materialization, Variables}
import datawrap.external.input.CsvSparkImport
import org.apache.spark.sql.types.StructType

object SviStatSource
    extends CsvSparkImport(
      header = true,
      delimiter = ","
    ) {

  private val schemaString: String = """
      |id		        string,
      |caller		    string,
      |called		    int,
      |tenantid		    string,
      |tenantname       string,
      |treeid		    string,
      |treename		    string,
      |start		    double,
      |started		    timestamp,
      |duration		    double,
      |blockread	    int,
      |endcause		    string,
      |gcid		        string,
      |cdin		        string,
      |cdout		    string,
      |transferredto	int,
      |forwardednumber	string,
      |reinvitednumber	string,
      |nodeid_used		string
      |""".stripMargin

  override val inputSchema: Option[StructType] = Some(StructType.fromDDL(schemaString))

  override val materialization: Materialization = Materialization.EPHEMERAL

  override val declaredVariables: Set[String] = Set("jour")

  override def getInputPath(variables: Variables): String =
    s"${AppConf.repertoireSourceDesFichiers}/svi/all/ResultatDGFIP_DailyStats_${variables("jour").replace("-", "")}.csv"

}
