package it.eng.au.sgsFlussoStoricoGas.dao

import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

trait Dao {
  val tablePath: String
  val columns: List[String]

  def readTable: DataFrame = {
    Environment.getSpark.sqlContext.read.parquet(tablePath)
  }

}
