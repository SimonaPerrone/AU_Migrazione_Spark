package it.eng.au.sgsFlussoStoricoGas.dao.perimetro

import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class SgsPerimetroUigSDao extends PerimetroDao {
  override val tableName: String = Environment.getSgsDBName++"."++Environment.getPerimetroUigSTableName
  override val tablePath: String = Environment.getPerimetroUigSPath
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

}
