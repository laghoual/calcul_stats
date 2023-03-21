package conf

import com.typesafe.config.Config
import datawrap.TConf

object AppConf extends TConf {

  lazy val appConf: Config                        = conf.getConfig("app")
  lazy val repertoireSourceDesFichiers: String    = appConf.getString("repertoireSourceDesFichiers")
  lazy val repertoireResultatsDesFichiers: String = appConf.getString("repertoireResultatsDesFichiers")
  lazy val calculChecksumDesExportsCsv: Boolean   = appConf.getBoolean("calculChecksumDesExportsCsv")
}
