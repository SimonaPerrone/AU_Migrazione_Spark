package it.eng.au.sgsFlussoStoricoGas.dao.rcugas

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasConnessioniDistr2PSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.DateType

class RcuGasConnessioniDistr2PDao extends Dao {
  override val tablePath: String = Environment.getRcugasConnessioniDistr2Path
  override val columns: List[String] = List(
    RcuGasConnessioniDistr2PSchema.n_id_pdr,
    RcuGasConnessioniDistr2PSchema.t_remi,
    RcuGasConnessioniDistr2PSchema.id_regione_climatica
  )

  def get(dateFlow: String): DataFrame = {
    Environment.getSpark.read.parquet(tablePath)
      .withColumn("dateFlow", lit(dateFlow).cast(DateType))
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn, when(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn).isNull, lit("1900-01-01").cast(DateType)).otherwise(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn).cast(DateType)))
      .withColumn(RcuGasConnessioniDistr2PSchema.d_data_fine_conn, when(col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn).isNull, lit("9999-12-31").cast(DateType)).otherwise(col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn).cast(DateType)))
      .filter(col("dateFlow").between(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn), col(RcuGasConnessioniDistr2PSchema.d_data_fine_conn)))
      .selectExpr(columns: _*)
  }
}
