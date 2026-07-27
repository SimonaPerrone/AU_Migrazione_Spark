package it.eng.au.sgsFlussoStoricoGas.dao.perimetro
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SaveMode}

class SgsPerimetroVtgSDao extends PerimetroDao {
  override val tableName: String = Environment.getSgsDBName++"."++Environment.getPerimetroVtgSTableName
  override val tablePath: String = Environment.getPerimetroVtgSPath
  override val columns: List[String] = SgsPerimetroSchema.getValues

  def writeParquet(df: DataFrame): Unit = {
    df
      .coalesce(5)
      .withColumn(SgsPerimetroSchema.executionId, lit(Environment.executionId))
      .selectExpr(columns: _*)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy(SgsPerimetroSchema.anno_mese_calcolo_perimetro.toString, SgsPerimetroSchema.executionId.toString)
      .parquet(tablePath)

    Environment.getSpark.sql(s"MSCK REPAIR TABLE $tableName")
  }

  def readLastForVtgAAggr(vtgAAggregationExecIds: Array[Long]): DataFrame = {
    Environment.getSpark.read.parquet(tablePath)
      .filter(col(SgsPerimetroSchema.executionId).isin(vtgAAggregationExecIds:_*))
  }

}
