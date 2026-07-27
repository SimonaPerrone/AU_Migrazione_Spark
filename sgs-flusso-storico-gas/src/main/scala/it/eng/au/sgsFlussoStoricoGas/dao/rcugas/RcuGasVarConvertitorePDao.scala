package it.eng.au.sgsFlussoStoricoGas.dao.rcugas

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasVarConvertitorePSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.DateType

class RcuGasVarConvertitorePDao extends Dao {

  override val tablePath: String = Environment.getRcugasVarConvertitiorePath
  override val columns: List[String] = List(
    RcuGasVarConvertitorePSchema.n_id_pdr,
    RcuGasVarConvertitorePSchema.t_matricola_convertitore,
    RcuGasVarConvertitorePSchema.t_data_inst_convertitore
  )

  def get(dateFlow: String): DataFrame = {
    Environment.getSpark.read.parquet(tablePath)
      .withColumn("dateFlow", lit(dateFlow).cast(DateType))
      .withColumn(RcuGasVarConvertitorePSchema.d_data_inizio, when(col(RcuGasVarConvertitorePSchema.d_data_inizio).isNull, lit("1900-01-01").cast(DateType)).otherwise(col(RcuGasVarConvertitorePSchema.d_data_inizio).cast(DateType)))
      .withColumn(RcuGasVarConvertitorePSchema.d_data_fine, when(col(RcuGasVarConvertitorePSchema.d_data_fine).isNull, lit("9999-12-31").cast(DateType)).otherwise(col(RcuGasVarConvertitorePSchema.d_data_fine).cast(DateType)))
      .filter(col("dateFlow").between(col(RcuGasVarConvertitorePSchema.d_data_inizio), col(RcuGasVarConvertitorePSchema.d_data_fine)))
      .selectExpr(columns: _*)
  }
}
