package it.eng.cdp_codprofstd_tds.dao

import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}


trait OutputDao extends Dao {
  val partitionCols: List[String]
  val hdfsOutput: String

  def writeParquet(df: DataFrame): Unit = {
    df.write.partitionBy(partitionCols: _*).mode(SaveMode.Append).parquet(hdfsOutput)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
