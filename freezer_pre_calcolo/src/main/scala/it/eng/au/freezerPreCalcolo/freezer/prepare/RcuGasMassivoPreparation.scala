package it.eng.au.freezerPreCalcolo.freezer.prepare

import it.eng.au.freezerPreCalcolo.schema._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}


/** Contains the methods used to prepare dataframes for subsequent joins and transformations. */
object RcuGasMassivoPreparation {

  /** Given RcugasMassivo dataframe [[df]], selects only active PDRs at the given [[freezeDateColumn]]. Specifically,
   * it filters PDRs for which [[freezeDateColumn]] is between [[d_data_inizio_for]] and [[data_fine_for]].
   *
   * @param df               RcugasMassivo dataframe
   * @param freezeDateColumn column containing the freezing date, used to select active PDRs
   * @return a dataframe containing active PDRs only.
   */
  def prepareActivePdr(df: DataFrame, freezeDateColumn: Column): DataFrame = {
    df.select(RcuGasMassivoSchema.n_id_pdr,
      RcuGasMassivoSchema.d_data_inizio_for,
      RcuGasMassivoSchema.data_fine_for)
      .withColumn(RcuGasMassivoSchema.d_data_inizio_for, to_date(coalesce(col(RcuGasMassivoSchema.d_data_inizio_for), lit("1492-12-31 00:00:00.0"))))
      .withColumn(RcuGasMassivoSchema.data_fine_for, to_date(coalesce(col(RcuGasMassivoSchema.data_fine_for), lit("2999-12-31 00:00:00.0"))))
      .where(freezeDateColumn.between(col(RcuGasMassivoSchema.d_data_inizio_for), col(RcuGasMassivoSchema.data_fine_for)))
      .select(RcuGasMassivoSchema.n_id_pdr)
      .distinct()
  }

  /** Performs a pre-processing of RcugasMassivo dataframe [[df]], by filtering out uninterpretable data
   * ([[n_id_pdr]] or [[n_id_fornitura]] null), or PDRs for which [[freezeDateColumn]] &lt; [[d_data_inizio_for]].
   *
   * @param df               RcugasMassivo dataframe
   * @param freezeDateColumn column containing the freezing date
   * @return Processed RcugasMassivo dataframe.
   */
  def prepareRcuGasMassivo(df: DataFrame, freezeDateColumn: Column): DataFrame = {
    df
      .selectExpr(RcuGasMassivoSchema.getValues: _*)
      .where(col(RcuGasMassivoSchema.n_id_pdr).isNotNull)
      .withColumn(RcuGasMassivoSchema.d_data_inizio_for, to_date(coalesce(col(RcuGasMassivoSchema.d_data_inizio_for), lit("1492-12-31 00:00:00.0"))))
      .withColumn(RcuGasMassivoSchema.data_fine_for, to_date(coalesce(col(RcuGasMassivoSchema.data_fine_for), lit("2999-12-31 00:00:00.0"))))
      .where(col(RcuGasMassivoSchema.n_id_fornitura).isNotNull && (col(RcuGasMassivoSchema.n_id_fornitura) =!= ""))
      .where(freezeDateColumn >= col(RcuGasMassivoSchema.d_data_inizio_for))
  }

  /** Pre-processes RcugasConnessioniDistr2 dataframe [[df]], by selecting PDRs for which [[freezeDateColumn]]
   * is between [[d_data_inizio_conn]] and [[d_data_fine_conn]].
   *
   * @param df               RcugasConnessioniDistr2 dataframe
   * @param freezeDateColumn column containing the freezing date
   * @return Processed RcugasConnessioniDistr2 dataframe.
   */
  def prepareIdRegClimatica(df: DataFrame, freezeDateColumn: Column): DataFrame = {
    df
      .select(RcuGasConnessioniDistr2Schema.n_id_pdr,
        RcuGasConnessioniDistr2Schema.id_regione_climatica,
        RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
        RcuGasConnessioniDistr2Schema.d_data_fine_conn)
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, to_date(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), lit("1492-12-31 00:00:00.0"))))
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, to_date(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), lit("2999-12-31 00:00:00.0"))))
      .where(freezeDateColumn.between(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), col(RcuGasConnessioniDistr2Schema.d_data_fine_conn)))
      .select(RcuGasConnessioniDistr2Schema.n_id_pdr, RcuGasConnessioniDistr2Schema.id_regione_climatica)
      .distinct()
  }

  /** Selects [[t_codice_pdr]] and [[n_idr_pdr]] columns and performs a distinct transformation on them.
   *
   * @param df RcugasMassivoFrozen dataframe, i.e. RcugasMassivo dataframe after the freezing procedure.
   */
  def prepareRcuGasMassivoFrozen(df: DataFrame): DataFrame = {
    df
      .select(RcuGasMassivoFrozenSchema.t_codice_pdr, RcuGasMassivoFrozenSchema.n_id_pdr)
      .distinct()
  }

  /** Pre-processes RcugasVarConvertitore dataframe, by selecting a subset of columns and performing
   * a distinct transformation on them.
   *
   * @param df - RcugasVarConvertitore dataframe
   * @return Processed RcugasVarConvertitore dataframe.
   */
  def prepareConv(df: DataFrame): DataFrame = {
    df.select(RcuGasVarConvertitoreSchema.n_id_pdr,
      RcuGasVarConvertitoreSchema.t_matricola_convertitore,
      RcuGasVarConvertitoreSchema.n_num_cifre_convertitore,
      RcuGasVarConvertitoreSchema.d_data_inizio,
      RcuGasVarConvertitoreSchema.d_data_fine)
      .withColumn(RcuGasVarConvertitoreSchema.d_data_inizio, col(RcuGasVarConvertitoreSchema.d_data_inizio).cast(TimestampType))
      .withColumn(RcuGasVarConvertitoreSchema.d_data_fine, col(RcuGasVarConvertitoreSchema.d_data_fine).cast(TimestampType))
  }

  /** Pre-processes RcugasVarMisuratore dataframe, by selecting a subset of columns and performing a rename
   * to match output dataframe columns names.
   *
   * @param df RcugasVarMisuratore dataframe
   * @return Processes RcugasVarMisuratore dataframe.
   */
  def prepareMis(df: DataFrame): DataFrame = {
    df.select(RcuGasVarMisuratoreSchema.n_id_pdr,
      RcuGasVarMisuratoreSchema.t_matricola_misuratore,
      RcuGasVarMisuratoreSchema.t_misuratore_integrato,
      RcuGasVarMisuratoreSchema.n_coeff_correzione,
      RcuGasVarMisuratoreSchema.n_num_cifre_misuratore,
      RcuGasVarMisuratoreSchema.d_data_inizio,
      RcuGasVarMisuratoreSchema.d_data_fine)
      .withColumnRenamed(RcuGasVarMisuratoreSchema.t_misuratore_integrato, RcuGasTechFrozenSchema.t_misuratore_integrato)
      .withColumnRenamed(RcuGasVarMisuratoreSchema.d_data_inizio, RcuGasTechFrozenSchema.data_inizio_tech)
      .withColumnRenamed(RcuGasVarMisuratoreSchema.d_data_fine, RcuGasTechFrozenSchema.data_fine_tech)
      .withColumn(RcuGasTechFrozenSchema.data_inizio_tech, col(RcuGasTechFrozenSchema.data_inizio_tech).cast(TimestampType))
      .withColumn(RcuGasTechFrozenSchema.data_fine_tech, col(RcuGasTechFrozenSchema.data_fine_tech).cast(TimestampType))
  }
}
