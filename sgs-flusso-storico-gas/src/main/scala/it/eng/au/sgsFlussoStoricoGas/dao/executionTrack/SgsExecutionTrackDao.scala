package it.eng.au.sgsFlussoStoricoGas.dao.executionTrack

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.SgsExecutionTrackSchema
import it.eng.au.sgsFlussoStoricoGas.utility.constants.TipoProcesso
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SaveMode}

class SgsExecutionTrackDao extends Dao {
  override val tablePath: String = Environment.getExecutionTrackPath
  override val columns: List[String] = SgsExecutionTrackSchema.getValues

  def writeParquet(df: DataFrame): Unit = {
    df
      .withColumn(SgsExecutionTrackSchema.d_data_competenza, col(SgsExecutionTrackSchema.d_data_competenza).cast(DateType))
      .withColumn(SgsExecutionTrackSchema.data_esecuzione, col(SgsExecutionTrackSchema.data_esecuzione).substr(1,10).cast(DateType))
      .selectExpr(columns:_*)
      .write
      .mode(SaveMode.Append)
      .parquet(tablePath)
  }

  def getExecIdForAggregation: Long = {
    Environment.getSpark.read.parquet(tablePath)
      .filter(col(SgsExecutionTrackSchema.tipo_processo) === TipoProcesso.P.toString)
      .select(SgsExecutionTrackSchema.executionId)
      .rdd
      .map(row => row.getLong(0))
      .max
  }

  def getExecIdForVtgAAggregation: Array[Long] = {
    val lb = Environment.startDateTime.toLocalDate.minusMonths(1).withDayOfMonth(1).toString
    val lastDayOfMonth =  Environment.startDateTime.toLocalDate.minusMonths(1).lengthOfMonth
    val ub = Environment.startDateTime.toLocalDate.minusMonths(1).withDayOfMonth(lastDayOfMonth).toString

    val lastExecIdRDD = Environment.getSpark.read.parquet(tablePath)
      .filter(col(SgsExecutionTrackSchema.tipo_processo) === TipoProcesso.P.toString)
      .filter(col(SgsExecutionTrackSchema.d_data_competenza).between(lit(lb).cast(DateType), lit(ub).cast(DateType)))
      .selectExpr(SgsExecutionTrackSchema.executionId)
      .rdd
      .map(row => row.getLong(0))

    val lastExecIdsValues = if (lastExecIdRDD.isEmpty()) {
      // Valore di default in caso di RDD vuoto
      Array(0L)
    } else {
      lastExecIdRDD.collect
    }

    lastExecIdsValues
  }

  def getExecIdForPublish: Long = {
    Environment.getSpark.read.parquet(tablePath).filter(col(SgsExecutionTrackSchema.tipo_processo) === TipoProcesso.A.toString)
      .selectExpr(SgsExecutionTrackSchema.executionId)
      .rdd
      .map(row => row.getLong(0))
      .max
  }
}
