package it.eng.au.portale_consumi_ee.dao.hive
import org.apache.spark.sql.{DataFrame, SparkSession}

object CheckPointUtils {

  /**
   * Write a DataFrame to a named checkpoint location under /tmp/checkpoints/{name}
   * Optionally include a suffix (e.g. annomese) to make the path unique
   */
  def writeCheckpoint(df: DataFrame, name: String, suffix: String = ""): Unit = {
    val finalName = if (suffix.isEmpty) name else s"${name}_$suffix"
    val path = s"/tmp/checkpoints/$finalName"
    df.write
      .mode("overwrite")
      .format("parquet")
      .save(path)
  }

  /**
   * Read a checkpoint DataFrame from the named location
   */
  def readCheckpoint(spark: SparkSession, name: String, suffix: String = ""): DataFrame = {
    val finalName = if (suffix.isEmpty) name else s"${name}_$suffix"
    val path = s"/tmp/checkpoints/$finalName"
    spark.read.parquet(path)
  }
}
