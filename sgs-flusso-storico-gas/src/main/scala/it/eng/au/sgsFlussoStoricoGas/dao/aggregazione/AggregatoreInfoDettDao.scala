package it.eng.au.sgsFlussoStoricoGas.dao.aggregazione

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.aggregazione.AggregatoreInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

class AggregatoreInfoDettDao extends Dao {
  override val tablePath: String = Environment.getAggregatoreInfoDettPath
  val tableName: String = Environment.getAggregatoreInfoDettTableName
  override val columns: List[String] = AggregatoreInfoDettSchema.getValues

  def write(df: DataFrame): Unit = {
    df
      .coalesce(10)
      .selectExpr(columns:_*)
      .write
      .partitionBy(AggregatoreInfoDettSchema.anno_mese, AggregatoreInfoDettSchema.execution_id)
      .mode(SaveMode.Append)
      .parquet(tablePath)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }

  def readLastPartition(lastExecId: Long): DataFrame = {
    Environment.getSpark.sqlContext.read.parquet(tablePath).filter(col(AggregatoreInfoDettSchema.execution_id) === lastExecId)
  }

}
