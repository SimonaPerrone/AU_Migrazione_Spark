package it.eng.cdp_codprofstd_tds.controller

import it.eng.cdp_codprofstd_tds.schema._
import it.eng.cdp_codprofstd_tds.utility.Constants.{DATA_CREAZIONE_FORMAT, DATE_FORMAT, TIMESTAMP_FORMAT}
import it.eng.cdp_codprofstd_tds.utility.Environment
import it.eng.cdp_codprofstd_tds.utility.Utility.notNullorEmpty
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Prepare {

  def prepareRcuGasMassivo(df: DataFrame, freezeDate: String): DataFrame = {
    df
      .filter(col(RcuGasMassivoSchema.t_codice_pdr).isNotNull)
      .withColumn("d_data_inizio_for_between", from_unixtime(unix_timestamp(coalesce(col(RcuGasMassivoSchema.d_data_inizio_for), lit("1900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .withColumn("data_fine_for_between", from_unixtime(unix_timestamp(coalesce(col(RcuGasMassivoSchema.data_fine_for), lit("2900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .filter(lit(freezeDate).between(col("d_data_inizio_for_between"), col("data_fine_for_between")))
      .selectExpr(RcuGasMassivoSchema.getValues: _*)
  }

  def prepareGasTds(df: DataFrame, startDate: Column, endDate: Column): DataFrame = {
    val window = Window.partitionBy(GasTdsSchema.cod_pdr).orderBy(col(GasTdsSchema.data_creazione).desc)
    df
      .filter(notNullorEmpty(col(GasTdsSchema.cod_pdr)) && notNullorEmpty(col(GasTdsSchema.data_creazione)))
      .withColumn("data_creazione_for_between", from_unixtime(unix_timestamp(col(GasTdsSchema.data_creazione), DATA_CREAZIONE_FORMAT), DATE_FORMAT))
      .withColumn(GasTdsSchema.data_creazione, from_unixtime(unix_timestamp(col(GasTdsSchema.data_creazione), DATA_CREAZIONE_FORMAT), TIMESTAMP_FORMAT))
      .filter(col("data_creazione_for_between").between(startDate, endDate))
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .selectExpr(GasTdsSchema.getValues: _*)
  }

  def prepareRcuGasConnessioniDistr2(df: DataFrame, freezeDate: String): DataFrame = {
    df
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, from_unixtime(unix_timestamp(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), lit("1900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, from_unixtime(unix_timestamp(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), lit("2900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .filter(lit(freezeDate).between(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), col(RcuGasConnessioniDistr2Schema.d_data_fine_conn)))
      .select(RcuGasConnessioniDistr2Schema.t_codice_pdr, RcuGasConnessioniDistr2Schema.t_remi, RcuGasConnessioniDistr2Schema.n_id_distr)
  }

  def prepareRcuGasBilanciamento(df: DataFrame, freezeDate: String): DataFrame = {
    df
      .withColumn(RcuGasBilanciamentoSchema.d_data_inizio, from_unixtime(unix_timestamp(coalesce(col(RcuGasBilanciamentoSchema.d_data_inizio), lit("1900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .withColumn(RcuGasBilanciamentoSchema.d_data_fine, from_unixtime(unix_timestamp(coalesce(col(RcuGasBilanciamentoSchema.d_data_fine), lit("2900-01-01 00:00:00.0")), TIMESTAMP_FORMAT), DATE_FORMAT))
      .filter(lit(freezeDate).between(col(RcuGasBilanciamentoSchema.d_data_inizio), col(RcuGasBilanciamentoSchema.d_data_fine)))
      .select(RcuGasBilanciamentoSchema.n_id_pdr, RcuGasBilanciamentoSchema.n_id_udb)
  }

  def preparePrtVsg(df: DataFrame, startDate: Column, endDate: Column): DataFrame = {
    df
      .withColumn(PrtVsgSchema.d_data_esecuzione, from_unixtime(unix_timestamp(col(PrtVsgSchema.d_data_esecuzione), TIMESTAMP_FORMAT), DATE_FORMAT))
      .filter(col(PrtVsgSchema.t_stato) === "F"
        && col(PrtVsgSchema.t_tipo_prestazione).isin("A40", "A01")
        && col(PrtVsgSchema.d_data_esecuzione).between(startDate, endDate))
      .select(PrtVsgSchema.t_codice_pdr, PrtVsgSchema.n_id_pratica, PrtVsgSchema.d_data_esecuzione)
  }

  def preparePrtVtg(df: DataFrame, startDate: Column, endDate: Column): DataFrame = {
    df
      .withColumn(PrtVtgSchema.d_data_dec, from_unixtime(unix_timestamp(col(PrtVtgSchema.d_data_dec), TIMESTAMP_FORMAT), DATE_FORMAT))
      .filter(col(PrtVtgSchema.t_stato).isin("F3", "F4")
        && col(PrtVtgSchema.d_data_dec).between(startDate, endDate))
      .select(PrtVtgSchema.t_codice_pdr, PrtVtgSchema.n_id_pratica, PrtVtgSchema.d_data_dec)
  }

  def preparePrtVtgAggRcu(df: DataFrame): DataFrame = {
    df.
      filter(col(PrtVtgAggRcuSchema.t_esito_agg_rcu) === "1")
      .select(PrtVtgAggRcuSchema.n_id_pratica)
  }

  def preparePrtVsgAggRcu(df: DataFrame): DataFrame = {
    df.
      filter(col(PrtVtgAggRcuSchema.t_esito_agg_rcu) === "1")
      .select(PrtVtgAggRcuSchema.n_id_pratica)
  }

  def prepareExclusionPdr(filePath: String): DataFrame = {
    Environment.getSpark.sqlContext
      .read
      .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      .format("csv")
      .schema(ExclusionPdrSchema.createSparkSchema())
      .load(filePath)
      .select(ExclusionPdrSchema.pdr)
  }
}
