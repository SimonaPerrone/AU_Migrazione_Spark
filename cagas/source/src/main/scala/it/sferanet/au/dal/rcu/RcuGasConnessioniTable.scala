package it.sferanet.au.dal.rcu

import it.sferanet.au.schema.RcuGasConnessioniDistr2PSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.TimestampType

import java.text.SimpleDateFormat

class RcuGasConnessioniTable(inputPath: String) extends Serializable {

  def get(): DataFrame = {
    Environment.getSqlContext.read
      .parquet(inputPath)
      .select(
        RcuGasConnessioniDistr2PSchema.t_codice_pdr.toString,
        RcuGasConnessioniDistr2PSchema.t_remi.toString,
        RcuGasConnessioniDistr2PSchema.d_data_inizio_conn.toString,
        RcuGasConnessioniDistr2PSchema.d_data_fine_conn.toString
      )
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn, when(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn).isNull, lit("1970-01-01 00:00:00.0")).otherwise(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn)))
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_fine_conn, when(col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn).isNull, lit("3000-01-01 00:00:00.0")).otherwise(col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn)))
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn, col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn).cast(TimestampType))
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_fine_conn, col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn).cast(TimestampType))
      .filter(col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn) >= lit("2025-09-30 00:00:00.0").cast(TimestampType))
  }
}

object RcuGasConnessioniTable {
  def format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
}
