package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

trait OutputDAO extends DAO {
  val tableName: String
  val partitionColumns: List[String]

  def write(df: DataFrame): Unit = {
    df
      .selectExpr(columns: _*)
      .repartition(10)
      .write
      .mode(SaveMode.Append)
      .partitionBy(partitionColumns: _*)
      .parquet(parquetPath)

    if (!Environment.isLocal) Environment.sqlContext.sql(s"MSCK REPAIR TABLE $tableName")
  }
}
