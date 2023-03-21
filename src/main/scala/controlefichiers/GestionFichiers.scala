package controlefichiers

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

object GestionFichiers {

  def writeSingleFile1(
      df: DataFrame,              // must be small
      format: String = "csv",     // csv, parquet
      sc: SparkContext,           // pass in spark.sparkContext
      tmpFolder: String,          // will be deleted, so make sure it doesn't already exist
      filename: String,           // the full filename you want outputted
      saveMode: String = "error", // Spark default is error, overwrite and append are also common
      delimiterCaractere: String = "|"
  ): Unit = {
    df.repartition(1)
      .write
      .mode(saveMode)
      .option("header", "true")
      .option("delimiter", delimiterCaractere)
      .format(format)
      .save(tmpFolder)

    val conf    = sc.hadoopConfiguration
    val src     = new Path(tmpFolder)
    val fs      = src.getFileSystem(conf)
    val oneFile = fs.listStatus(src).map(x => x.getPath.toString).find(x => x.endsWith(format))
    val srcFile = new Path(oneFile.getOrElse(""))
    val dest    = new Path(filename)
    val tmp     = new Path(tmpFolder)
    if (fs.exists(dest) && fs.isFile(dest))
      fs.delete(dest, true)
    fs.rename(srcFile, dest)
    if (fs.exists(tmp) && fs.isDirectory(tmp))
      fs.delete(tmp, true)
  }

}
