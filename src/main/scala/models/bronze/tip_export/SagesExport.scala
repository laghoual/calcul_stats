package models.bronze.tip_export

import conf.AppConf
import conf.AppConf.repertoireResultatsDesFichiers
import datawrap.core.{Materialization, SparkModel, Variables}
import datawrap.external.output.SingleCsvFileSparkExport
import models.bronze.tip.Croisements
import org.apache.spark.sql.DataFrame
import stats.ConstantesRepertoires.DATAVIZSAGESREP

object SagesExport
    extends SingleCsvFileSparkExport(
      tempFolder = repertoireResultatsDesFichiers + "/all/tmp/",
      exportedFilePath = DATAVIZSAGESREP,
      delimiter = ";",
      header = true,
      printChecksum = AppConf.calculChecksumDesExportsCsv
    ) {

  override val declaredVariables: Set[String] = Set()

  override val inputs: Set[SparkModel] = Set(Croisements)

  override val materialization: Materialization = Materialization.EPHEMERAL

  override protected def compute(implicit variables: Variables): DataFrame = {
    Croisements.selectExpr("SAGES").distinct
  }

}
