package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.FlowCdpDatiPrelievoGas
import it.eng.au.aggregatoreConsumiCdp.dao.cdp.CaFinalLikeCodProfDao
import it.eng.au.aggregatoreConsumiCdp.schema.{CaFinalLikeSchema, CaFinalSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.TIMESTAMP_FORMAT
import it.eng.au.aggregatoreConsumiCdp.utility.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object CaAggds extends FlowCdpDatiPrelievoGas {
  override def getCa(): DataFrame = {
    val caFinalLike = new CaFinalLikeCodProfDao().readPartition(Environment.getCodProfExecutionId)
    convertCaLikeInCaFinal(caFinalLike)
  }

  override def specificTransform(caFinal: DataFrame): DataFrame = {
    caFinal
      .withColumn(OutputHiveSchema.udd_oggetto_swithcing, lit(""))
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(trunc(add_months(col(OutputHiveSchema.d_data_rif), 1), "Month"), TIMESTAMP_FORMAT), TIMESTAMP_FORMAT))
      .selectExpr(OutputHiveSchema.getValues: _*)
  }

  def convertCaLikeInCaFinal(caFinalLike: DataFrame): DataFrame = {
    caFinalLike
      .withColumn(CaFinalLikeSchema.cat_uso, coalesce(col(CaFinalLikeSchema.cat_uso_forced), col(CaFinalLikeSchema.cat_uso)))
      .withColumn(CaFinalLikeSchema.cod_prof_prel_std_calc, coalesce(col(CaFinalLikeSchema.cod_prof_prel_std_forced), col(CaFinalLikeSchema.cod_prof_prel_std_calc)))
      .withColumn(CaFinalLikeSchema.zona_climatica, coalesce(col(CaFinalLikeSchema.zona_climatica_forced), col(CaFinalLikeSchema.zona_climatica)))
      .withColumn(CaFinalLikeSchema.classe_prelievo, coalesce(col(CaFinalLikeSchema.classe_prelievo_forced), col(CaFinalLikeSchema.classe_prelievo)))
      .withColumn(CaFinalLikeSchema.prelievo_annuo_prev, coalesce(col(CaFinalLikeSchema.prelievo_annuo_prev_forced), col(CaFinalLikeSchema.prelievo_annuo_prev)))
      .withColumn(CaFinalLikeSchema.trattamento, coalesce(col(CaFinalLikeSchema.trattamento_forced), col(CaFinalLikeSchema.trattamento)))
      .drop(col(CaFinalLikeSchema.cod_prof_prel_std))
      .withColumnRenamed(CaFinalLikeSchema.n_id_distr, CaFinalSchema.n_id_distr)
      .withColumnRenamed(CaFinalLikeSchema.n_id_udd, CaFinalSchema.n_id_az_udd)
      .withColumnRenamed(CaFinalLikeSchema.n_id_udb, CaFinalSchema.n_id_udb)
      .withColumnRenamed(CaFinalLikeSchema.cod_remi, CaFinalSchema.codice_remi)
      .withColumnRenamed(CaFinalLikeSchema.codice_pdr, CaFinalSchema.codice_pdr)
      .withColumnRenamed(CaFinalLikeSchema.cat_uso, CaFinalSchema.cat_uso)
      .withColumnRenamed(CaFinalLikeSchema.classe_prelievo, CaFinalSchema.classe_prelievo)
      .withColumnRenamed(CaFinalLikeSchema.zona_climatica, CaFinalSchema.zona_climatica)
      .withColumnRenamed(CaFinalLikeSchema.id_regione_climatica, CaFinalSchema.id_reg_clim)
      .withColumnRenamed(CaFinalLikeSchema.cod_prof_prel_std_calc, CaFinalSchema.cod_prof_prel_std)
      .withColumnRenamed(CaFinalLikeSchema.prelievo_annuo_prev, CaFinalSchema.prelievo_annuo_prev)
      .withColumnRenamed(CaFinalLikeSchema.trattamento, CaFinalSchema.trattamento)
      .withColumnRenamed(CaFinalLikeSchema.anno_competenza, CaFinalSchema.anno_competenza)
      .withColumnRenamed(CaFinalLikeSchema.massivo_freeze_execution_id, CaFinalSchema.massivo_freeze_executionid)
      .withColumnRenamed(CaFinalLikeSchema.massivo_freeze_date, CaFinalSchema.freeze_date)
      .withColumnRenamed(CaFinalLikeSchema.execution_id, CaFinalSchema.executionid)
      .withColumn(CaFinalSchema.anno_competenza, col(CaFinalSchema.anno_competenza).cast(StringType))
      .withColumn(CaFinalSchema.tipo_trasmissione, lit("AGG_DS"))
      .withColumn(CaFinalSchema.piva_distr, lit(""))
      .withColumn(CaFinalSchema.piva_udd, lit(""))
      .withColumn(CaFinalSchema.piva_udb, lit(""))
      .selectExpr(CaFinalSchema.getValues: _*)
  }
}
