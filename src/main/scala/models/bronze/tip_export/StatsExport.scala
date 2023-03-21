package models.bronze.tip_export

import conf.AppConf
import conf.AppConf.repertoireResultatsDesFichiers
import datawrap.core.{Materialization, SparkModel, Variables}
import datawrap.external.output.SingleCsvFileSparkExport
import models.bronze.tip.Croisements
import org.apache.spark.sql.DataFrame
import stats.ConstantesColonnes.COLSFINAL
import stats.ConstantesRepertoires.DATAVIZSTATSREP

object StatsExport
    extends SingleCsvFileSparkExport(
      tempFolder = repertoireResultatsDesFichiers + "/all/tmp/",
      exportedFilePath = DATAVIZSTATSREP,
      header = true,
      delimiter = ";",
      printChecksum = AppConf.calculChecksumDesExportsCsv
    ) {

  override val declaredVariables: Set[String] = Set()

  override val inputs: Set[SparkModel] = Set(Croisements)

  override val materialization: Materialization = Materialization.EPHEMERAL

  override protected def compute(implicit variables: Variables): DataFrame = {
    Croisements.selectExpr(COLSFINAL: _*).distinct
  }

}
