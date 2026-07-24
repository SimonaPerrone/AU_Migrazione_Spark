package it.eng.au.gsvAggregatoreConsumi.dao.gsv

import it.eng.au.gsvAggregatoreConsumi.dao.Dao
import it.eng.au.gsvAggregatoreConsumi.schema.gsv.GsvConsAggrSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.apache.spark.sql.types.{DecimalType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode}

class GsvConsAggrDAO extends Dao {

  override val columns: List[String] = GsvConsAggrSchema.getValues
  override val tablePath: String = Environment.getGsvConsAggregatoPath
  override val tableName: String = Environment.getGsvConsAggregatoTable
  private val sqoopDateFormat: String = "yyyy-MM-dd HH:mm:ss.SSS"

  def writeOnHive (df: DataFrame): Unit = {
    df
      .withColumn(GsvConsAggrSchema.n_id_gsv5_cons_richiesta, col(GsvConsAggrSchema.n_id_gsv5_cons_richiesta).cast(DecimalType(18,0)))
      .withColumn(GsvConsAggrSchema.n_id_gsv5_cons_aggr, col(GsvConsAggrSchema.n_id_gsv5_cons_aggr).cast(DecimalType(18,0)))
      .withColumn(GsvConsAggrSchema.n_consumo_mese, col(GsvConsAggrSchema.n_consumo_mese).cast(DecimalType(38,18)))
      .withColumn(GsvConsAggrSchema.t_giorni_mese, col(GsvConsAggrSchema.t_giorni_mese).cast(StringType))
      .withColumn(GsvConsAggrSchema.n_execution_id, lit(Environment.executionId).cast(DecimalType(18,0)))
      .withColumn(GsvConsAggrSchema.d_data_creazione, lit(Environment.startDateTime.toString).cast(TimestampType))
      .withColumn(GsvConsAggrSchema.d_data_creazione, date_format(col(GsvConsAggrSchema.d_data_creazione), sqoopDateFormat))
      .selectExpr(GsvConsAggrSchema.getValues:_*)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("nullValue", "NULL")
      .save(tablePath)


    Environment.getSpark.sql(s"REFRESH $tableName")

  }

}
