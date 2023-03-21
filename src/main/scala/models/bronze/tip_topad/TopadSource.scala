package models.bronze.tip_topad

import conf.AppConf
import datawrap.core.{Materialization, SparkModel, Variables}
import org.apache.spark.sql.DataFrame
import utils.FonctionsUtiles

object TopadSource extends SparkModel {

  override val inputs: Set[SparkModel] = Set()

  override val materialization: Materialization = Materialization.EPHEMERAL

  override val declaredVariables: Set[String] = Set()

  override protected def compute(implicit variables: Variables): DataFrame = {
    val sourcePath = s"${AppConf.repertoireSourceDesFichiers}/topad/data/last"

    FonctionsUtiles.readFileComplet(spark, null, sourcePath, ",", headerStatus = false)
  }

}
