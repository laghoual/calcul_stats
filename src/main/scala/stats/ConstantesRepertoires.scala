package stats

import launcher.Run.{repertoireResultatsDesFichiers, repertoireSourceDesFichiers}

//import stats.Run.test
object ConstantesRepertoires {
  // numerotation taxation
  val REGLENUMEROTAXATIONREP: String = repertoireSourceDesFichiers + "/numerotation/numerotation_taxation.csv"
  val REGLENUMEROTAXATIONINTREP: String =
    repertoireSourceDesFichiers + "/numerotation/numerotation_taxation_international.csv"
  // topad
  val TOPADREP: String     = repertoireSourceDesFichiers + "/topad/data/last"
  val TOPADCONFREP: String = repertoireSourceDesFichiers + "/topad/conf"
  // suivi
  val REPERTOIRESUIVIFICHIERS: String = repertoireResultatsDesFichiers + "/suivi"
  val SUIVIALLFILESREP: String        = REPERTOIRESUIVIFICHIERS + "/suiviallfiles.csv"
  val SUIVITRAITEMENTREP: String      = REPERTOIRESUIVIFICHIERS + "/suivifile.csv"

  // repertoire pour export en csv de la table de fichiers
  val ALLSTATSREP: String = repertoireResultatsDesFichiers + "/all/stats/stats.csv"
  val ALLSAGESREP: String = repertoireResultatsDesFichiers + "/all/sages/sages.csv"
  // dataviz
  val DATAVIZREP: String      = repertoireResultatsDesFichiers + "/dataviz"
  val DATAVIZSTATSREP: String = DATAVIZREP + "/stats/stats.csv"
  val DATAVIZSAGESREP: String = DATAVIZREP + "/sages/sages.csv"
}
