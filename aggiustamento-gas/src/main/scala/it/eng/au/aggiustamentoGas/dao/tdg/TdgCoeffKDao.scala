package it.eng.au.aggiustamentoGas.dao.tdg

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasMassivoP
import it.eng.au.aggiustamentoGas.schema.tdg.TdgCoeffKSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

/** Tabella importata tramite sqoop, utilizzata per l'estrazione del coefficiente K */
class TdgCoeffKDao extends Dao {
  override val parquetPath: String = Environment.getTdgCoeffKPath
  override val columns: List[String] = List(
    TdgCoeffKSchema.n_id_tdg4_coeffk,
    TdgCoeffKSchema.n_id_pdr,
    TdgCoeffKSchema.n_val_k,
    TdgCoeffKSchema.d_data_inizio,
    TdgCoeffKSchema.d_data_fine,
    TdgCoeffKSchema.d_data_rif,
    TdgCoeffKSchema.t_tipo_op,
    TdgCoeffKSchema.d_data_inserimento,
    TdgCoeffKSchema.d_data_aggiornamento
  )
  private val tdgSchema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))

  def get(rcuGasMassivo: RDD[RcuGasMassivoP], startFlow: String, endFlow: String): DataFrame = {
    val tdgDF = Environment.getSpark.sqlContext.read.format("csv").option("sep", ";").schema(tdgSchema).load(parquetPath)
      .withColumn(TdgCoeffKSchema.n_val_k, lit(col(TdgCoeffKSchema.n_val_k)).cast(DoubleType))

    val df = prepare(tdgDF, rcuGasMassivo)

    filterDfWithStartEndDate(
      df
      , TdgCoeffKSchema.d_data_inizio
      , TdgCoeffKSchema.d_data_fine
      , "yyyy-MM-dd HH:mm:ss.S"
      , startFlow
      , endFlow
      , "yyyyMM"
    )

  }

  def prepare(df: DataFrame, rcuGasMassivo: RDD[RcuGasMassivoP]): DataFrame = {
    val nIdPdr = "massivo_n_id_pdr"
    val tCodicePdr = "massivo_t_codice_pdr"

    val rcuMassivo = Environment.getSpark.sqlContext.createDataFrame(rcuGasMassivo.map(rcu => (rcu.nIdPdr, rcu.tCodicePdr)).distinct())
      .withColumnRenamed("_1", nIdPdr)
      .withColumnRenamed("_2", tCodicePdr)

    df
      .join(rcuMassivo, col(TdgCoeffKSchema.n_id_pdr) === col(nIdPdr), "inner")
      .drop(nIdPdr, TdgCoeffKSchema.n_id_pdr)
      .withColumnRenamed(tCodicePdr, TdgCoeffKSchema.n_id_pdr)
      .selectExpr(columns:_*)

  }

}
