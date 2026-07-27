package it.eng.au.sgsFlussoStoricoGas.dao.rcugas

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.RcuGasVarMisuratorePSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.DateType

class RcuGasVarMisuratorePDao extends Dao {

  override val tablePath: String = Environment.getRcugasVarMisuratorePath
  override val columns: List[String] = List(
    RcuGasVarMisuratorePSchema.n_id_pdr,
    RcuGasVarMisuratorePSchema.t_matricola_misuratore,
    RcuGasVarMisuratorePSchema.t_data_inst_misuratore,
    RcuGasVarMisuratorePSchema.t_misuratore_integrato,
    RcuGasVarMisuratorePSchema.t_classe_misuratore,
    RcuGasVarMisuratorePSchema.t_telegestito,
    RcuGasVarMisuratorePSchema.t_presenza_convertitore,
    RcuGasVarMisuratorePSchema.t_tipo_misuratore,
    RcuGasVarMisuratorePSchema.n_coeff_correzione
  )

  def get(dateFlow: String): DataFrame = {
    Environment.getSpark.read.parquet(tablePath)
      .withColumn("dateFlow", lit(dateFlow).cast(DateType))
      .withColumn(RcuGasVarMisuratorePSchema.d_data_inizio, when(col(RcuGasVarMisuratorePSchema.d_data_inizio).isNull, lit("1900-01-01").cast(DateType)).otherwise(col(RcuGasVarMisuratorePSchema.d_data_inizio).cast(DateType)))
      .withColumn(RcuGasVarMisuratorePSchema.d_data_fine, when(col(RcuGasVarMisuratorePSchema.d_data_fine).isNull, lit("9999-12-31").cast(DateType)).otherwise(col(RcuGasVarMisuratorePSchema.d_data_fine).cast(DateType)))
      .filter(col("dateFlow").between(col(RcuGasVarMisuratorePSchema.d_data_inizio), col(RcuGasVarMisuratorePSchema.d_data_fine)))
      .selectExpr(columns: _*)
  }
}
