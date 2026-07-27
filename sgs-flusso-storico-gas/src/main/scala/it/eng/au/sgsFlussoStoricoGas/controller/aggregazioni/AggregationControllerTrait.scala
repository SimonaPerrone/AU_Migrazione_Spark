package it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni

import it.eng.au.sgsFlussoStoricoGas.schema.aggregazione.AggregatoreInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.DailyConsumptionSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.schema.rcugas.{RcuGasConnessioniDistr2PSchema, RcuGasVarConvertitorePSchema, RcuGasVarMisuratorePSchema}
import it.eng.au.sgsFlussoStoricoGas.utility.constants.FieldConstants.UDB
import it.eng.au.sgsFlussoStoricoGas.utility.constants.{StatoAggregazione, StatoPerimetro, TipoFlusso}
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, broadcast, col, concat, date_sub, dayofmonth, lit, lpad, md5, month, round, sum, when, year}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

trait AggregationControllerTrait {

  val tipoFlusso = ""
  val tipoPratica = ""
  val nomeServizio = ""
  val pivaUtenteDest = ""

  def getReducedConsumptions(perimetro: DataFrame, consumptions: DataFrame): DataFrame = {

    val pdrList = perimetro.select(SgsPerimetroSchema.t_codice_pdr).distinct.rdd.collect.map(row => row.getString(0))

    consumptions.filter(col(DailyConsumptionSchema.pdr).isin(pdrList:_*))
  }

  def getAggregatoreInfoDett(perimetro: DataFrame, consumptions: DataFrame, rcuGasVarMisuratore: DataFrame, rcuGasVarConvertitore: DataFrame, rcuGasConnessioniDistr2: DataFrame): DataFrame = {

    val dayOfMonth = "dayOfMonth"
    val pivotPrefix = "PRELIEVO_"
    val partitionColumns = AggregatoreInfoDettSchema.getValues.filter(v => !v.contains("prelievo_")).diff(Seq(AggregatoreInfoDettSchema.id_aggregatore_info.toString, AggregatoreInfoDettSchema.execution_id.toString))
    val windowMonth = Window.partitionBy(partitionColumns.map(col): _*)
    val prelievoColumns = AggregatoreInfoDettSchema.getValues.filter(v => "prelievo_\\d+".r.findFirstIn(v).isDefined)
    val excludeColumns = prelievoColumns ++ Seq(AggregatoreInfoDettSchema.id_aggregatore_info.toString ,AggregatoreInfoDettSchema.execution_id.toString)
    val groupingColumns = AggregatoreInfoDettSchema.getValues.diff(excludeColumns).map(col)
    val keyCols = Seq(AggregatoreInfoDettSchema.tipo_flusso.toString, AggregatoreInfoDettSchema.tipo_pratica.toString, AggregatoreInfoDettSchema.nome_servizio.toString, AggregatoreInfoDettSchema.piva_distr.toString, AggregatoreInfoDettSchema.n_id_pratica.toString, AggregatoreInfoDettSchema.cod_pdr.toString, AggregatoreInfoDettSchema.anno_mese.toString)
    val dataDecorrFilterLb = if (tipoFlusso.equals(TipoFlusso.S.toString)) {
      date_sub(add_months(col(SgsPerimetroSchema.d_data_decorrenza), -13), 0)
    }
    else
      date_sub(add_months(col(SgsPerimetroSchema.d_data_decorrenza), -1), 0)

    val dataDecorrFilterUb = if (tipoFlusso.equals(TipoFlusso.S.toString)) {
      date_sub(add_months(col(SgsPerimetroSchema.d_data_decorrenza), -2), 0)
    }
    else
      date_sub(add_months(col(SgsPerimetroSchema.d_data_decorrenza), -1), 0)

    //broadcast
    val broadcastPerimetro = broadcast(perimetro)

    broadcastPerimetro.filter(col(SgsPerimetroSchema.t_stato_perimetro) === StatoPerimetro.OK.toString)
      .join(consumptions, col(DailyConsumptionSchema.pdr).equalTo(col(SgsPerimetroSchema.t_codice_pdr)), "left")
      .withColumn("data_decorrenza_lb", dataDecorrFilterLb)
      .withColumn("data_decorrenza_ub", dataDecorrFilterUb)
      .withColumn("data_decorrenza_lb_am", concat(year(col("data_decorrenza_lb")), lpad(month(col("data_decorrenza_lb")), 2, "0")).cast(IntegerType))
      .withColumn("data_decorrenza_ub_am", concat(year(col("data_decorrenza_ub")), lpad(month(col("data_decorrenza_ub")), 2, "0")).cast(IntegerType))
      .filter(col(DailyConsumptionSchema.annoMese).cast(IntegerType).between(col("data_decorrenza_lb_am"), col("data_decorrenza_ub_am")))
      .drop("data_decorrenza_lb", "data_decorrenza_ub", "data_decorrenza_lb_am", "data_decorrenza_ub_am")
      .join(rcuGasVarMisuratore, Seq(SgsPerimetroSchema.n_id_pdr.toString), "left")
      .join(rcuGasVarConvertitore, Seq(SgsPerimetroSchema.n_id_pdr.toString), "left")
      .join(rcuGasConnessioniDistr2, Seq(SgsPerimetroSchema.n_id_pdr.toString), "left")
      .withColumn(AggregatoreInfoDettSchema.tipo_flusso, lit(tipoFlusso))
      .withColumn(AggregatoreInfoDettSchema.tipo_pratica, lit(tipoPratica))
      .withColumn(AggregatoreInfoDettSchema.nome_servizio, lit(nomeServizio))
      .withColumn(AggregatoreInfoDettSchema.t_stato_dett,
        when((col(SgsPerimetroSchema.piva_udb_entrante).equalTo(col(SgsPerimetroSchema.piva_udb_uscente))
          or col(SgsPerimetroSchema.piva_udb_entrante).equalTo(col(SgsPerimetroSchema.piva_udd_entrante)))
          and col(AggregatoreInfoDettSchema.nome_servizio)===UDB, lit(StatoAggregazione.KO_A.toString)))
      .withColumn(AggregatoreInfoDettSchema.piva_utente_dest, col(pivaUtenteDest))
      .withColumn(AggregatoreInfoDettSchema.anno_mese_comp, concat(lit(Environment.startDateTime.getYear), lpad(lit(Environment.startDateTime.getMonthValue), 2, "0")))
      .withColumn(AggregatoreInfoDettSchema.execution_id_perimetro_sgs, col(SgsPerimetroSchema.executionId).cast(LongType))
      .withColumn(AggregatoreInfoDettSchema.cod_pdr, col(SgsPerimetroSchema.t_codice_pdr).cast(StringType))
      .withColumn(AggregatoreInfoDettSchema.id_reg_clim, col(RcuGasConnessioniDistr2PSchema.id_regione_climatica).cast(StringType))
      .withColumnRenamed(SgsPerimetroSchema.n_id_pratica, AggregatoreInfoDettSchema.n_id_pratica)
      .withColumnRenamed(SgsPerimetroSchema.d_data_decorrenza, AggregatoreInfoDettSchema.d_data_decorrenza)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_matricola_misuratore, AggregatoreInfoDettSchema.matr_mis)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_data_inst_misuratore, AggregatoreInfoDettSchema.data_inst_mis)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_misuratore_integrato, AggregatoreInfoDettSchema.classe_mis_int)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_classe_misuratore, AggregatoreInfoDettSchema.classe_gruppo_mis)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_telegestito, AggregatoreInfoDettSchema.telegestione)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_presenza_convertitore, AggregatoreInfoDettSchema.pre_conv)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.t_tipo_misuratore, AggregatoreInfoDettSchema.tipo_mis)
      .withColumnRenamed(RcuGasVarMisuratorePSchema.n_coeff_correzione, AggregatoreInfoDettSchema.coeff_corr)
      .withColumnRenamed(RcuGasVarConvertitorePSchema.t_matricola_convertitore, AggregatoreInfoDettSchema.matr_conv)
      .withColumnRenamed(RcuGasVarConvertitorePSchema.t_data_inst_convertitore, AggregatoreInfoDettSchema.data_inst_conv)
      .withColumnRenamed(RcuGasConnessioniDistr2PSchema.t_remi, AggregatoreInfoDettSchema.cod_remi)
      .withColumnRenamed(DailyConsumptionSchema.pivaDistr, AggregatoreInfoDettSchema.piva_distr)
      .withColumnRenamed(DailyConsumptionSchema.treatment, AggregatoreInfoDettSchema.trattamento)
      .withColumnRenamed(DailyConsumptionSchema.annoMese, AggregatoreInfoDettSchema.anno_mese)
      .withColumn(AggregatoreInfoDettSchema.prelievo_aggregato, round(sum(col(DailyConsumptionSchema.value)).over(windowMonth)).cast(IntegerType))
      .withColumn(dayOfMonth, concat(lit(pivotPrefix), dayofmonth(col(DailyConsumptionSchema.date))))
      .groupBy(groupingColumns: _*)
      .pivot(dayOfMonth, (1 to 31).map(pivotPrefix + _))
      .agg(round(sum(col(DailyConsumptionSchema.value))).cast(IntegerType))
      .withColumn(AggregatoreInfoDettSchema.execution_id, lit(Environment.executionId))
      .withColumn(AggregatoreInfoDettSchema.id_aggregatore_info, md5(concat(keyCols.map(col):_*).cast(StringType)))
      .selectExpr(AggregatoreInfoDettSchema.getValues:_*)
  }

}
