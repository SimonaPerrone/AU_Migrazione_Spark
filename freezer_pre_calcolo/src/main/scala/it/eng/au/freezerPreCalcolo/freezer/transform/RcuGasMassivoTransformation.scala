package it.eng.au.freezerPreCalcolo.freezer.transform

import it.eng.au.freezerPreCalcolo.schema.{RcuGasConnessioniDistr2Schema, RcuGasMassivoFrozenSchema, RcuGasMassivoSchema, RcuGasVarConvertitoreSchema, RcuGasVarMisuratoreSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, lit, when}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext, functions}


object RcuGasMassivoTransformation {

  /** Contains the operations needed to get final dataframe (RcugasMassivoFrozen) from processed input dataframes.
   *
   * @param rcuGasMassivo base RcugasMassivo dataframe
   * @param activePdr dataframe containing the subset of active PDRs
   * @param idRegClimatica dataframe containing the climatic region for each PDR
   * @param freezeDateColumn column containing the freezing date
   * @param executionId current run timestamp
   * @return processed (i.e. "frozen") RcugasMassivo dataframe.
   */
  def transform(rcuGasMassivo: DataFrame,
                activePdr: DataFrame,
                idRegClimatica: DataFrame,
                freezeDateColumn: Column,
                executionId: String): DataFrame = {

    val rcuGasMassivoActivePdr = getActivePdr(rcuGasMassivo, activePdr, freezeDateColumn)

    getRegioneClimatica(rcuGasMassivoActivePdr, idRegClimatica, freezeDateColumn, executionId)
  }

  /** Selects active PDRs among PDRs in [[rcuGasMassivo]] dataframe.
   *
   * @param rcuGasMassivo base dataframe which get filtered
   * @param activePdr dataframe containing the subset of active PDRs to be selected
   * @param freezeDateColumn column containing the freezing date
   * @return [[rcuGasMassivo]] dataframe with active PDrs only.
   */
  def getActivePdr(rcuGasMassivo: DataFrame, activePdr: DataFrame, freezeDateColumn: Column): DataFrame = {
    val windowSpec = Window.partitionBy(col(RcuGasMassivoSchema.t_codice_pdr))
    val maxDataFineForCase = functions.max(col(RcuGasMassivoSchema.data_fine_for)).over(windowSpec)

    rcuGasMassivo
      .join(activePdr, rcuGasMassivo(RcuGasMassivoSchema.n_id_pdr) === activePdr(RcuGasMassivoSchema.n_id_pdr))
      .drop(activePdr(RcuGasMassivoSchema.n_id_pdr))
      .withColumn(RcuGasMassivoSchema.data_fine_for, when(col(RcuGasMassivoSchema.data_fine_for) === maxDataFineForCase
        && col(RcuGasMassivoSchema.data_fine_for) > freezeDateColumn, null).otherwise(col(RcuGasMassivoSchema.data_fine_for)))
  }

  /** Adds climatic region field to base RcugasMassivo dataframe.
   *
   * @param rcuGasMassivoActivePdr base RcugasMassivo dataframe
   * @param idRegClimatica dataframe containing climatic region field
   * @param freezeDateColumn column containing the freezing date
   * @param executionId current run timestamp
   * @return processed RcugasMassivo dataframe.
   */
  def getRegioneClimatica(rcuGasMassivoActivePdr: DataFrame, idRegClimatica: DataFrame, freezeDateColumn: Column, executionId: String): DataFrame = {
    rcuGasMassivoActivePdr.
      join(idRegClimatica, rcuGasMassivoActivePdr(RcuGasMassivoSchema.n_id_pdr) === idRegClimatica(RcuGasConnessioniDistr2Schema.n_id_pdr), "left_outer")
      .drop(idRegClimatica(RcuGasConnessioniDistr2Schema.n_id_pdr))
      .withColumn(RcuGasMassivoFrozenSchema.d_data_inizio_for, col(RcuGasMassivoFrozenSchema.d_data_inizio_for).cast(TimestampType))
      .withColumn(RcuGasMassivoFrozenSchema.data_fine_for, col(RcuGasMassivoFrozenSchema.data_fine_for).cast(TimestampType))
      .withColumn(RcuGasMassivoFrozenSchema.freeze_date, freezeDateColumn.cast(TimestampType))
      .withColumn(RcuGasMassivoFrozenSchema.execution_id, lit(executionId).cast(LongType))
  }
}
