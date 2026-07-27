package it.eng.au.aggiustamentoGas.dao

import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val parquetPath: String
  val columns: List[String]

  def readParquet: DataFrame = {
    Environment.getSpark.sqlContext.read.parquet(parquetPath)
      .selectExpr(columns:_*)
  }

}
