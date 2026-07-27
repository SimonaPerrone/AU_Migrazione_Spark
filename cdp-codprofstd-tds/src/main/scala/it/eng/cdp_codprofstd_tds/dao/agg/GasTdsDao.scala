package it.eng.cdp_codprofstd_tds.dao.agg

import it.eng.cdp_codprofstd_tds.dao.Dao
import it.eng.cdp_codprofstd_tds.schema.GasTdsSchema
import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.spark.sql.DataFrame

import scala.math.ceil

class GasTdsDao extends Dao {
  override val tableName: String = ""
  override val columns: List[String] = GasTdsSchema.getValues

  override def readParquet(parquetPath: String): DataFrame = {
    val df = Environment.getSpark.sqlContext.read.parquet(parquetPath).selectExpr(columns: _*)
    df.coalesce(ceil(df.rdd.getNumPartitions.toFloat / 100).toInt)
  }
}
