package it.eng.au.freezerPreCalcolo.dao.rcugas

import it.eng.au.freezerPreCalcolo.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

/** Provides an abstract interface for classes that need to write data to HDFS. */
trait FrozenDAO extends DAO {
  override val parquetPath = ""
  /** The column whereby output data is partitioned. */
  val partitionCols: List[String]
  /** HDFS path where data is written into. */
  val hdfsOutput: String
  /** Table name in Hive. */
  val tableName: String

  /** Writes data stored in [[df]] into HDFS. Specifically, data is partitioned through
   * [[partitionCols]] and stored in [[hdfsOutput]] path.
   *
   * @param df - [[DataFrame]] to be exported.
   */
  def writeParquet(df: DataFrame): Unit = {
    df.write.partitionBy(partitionCols: _*).mode(SaveMode.Append).parquet(hdfsOutput)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
