package it.sferanet.au.controller.caFinal

import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, coalesce, col, lit}
import org.apache.spark.sql.types.{LongType, StringType}

class CaFinalController {
  /** Le forzature validate nella ca_pre_final sovrascrivono la rispettiva colonna originale. Inoltre vengono aggiunte le informazioni sulle partite iva,
   * utilizzante nel processo di pubblicazione dei consumi (vedere progetto aggregatore-cdp). */
  def get(caPreFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame): DataFrame = {
    val caFinal = caPreFinal
      .withColumn(CaPreFinalSchema.prelievo_annuo_prev, coalesce(col(CaPreFinalSchema.prelievo_annuo_prev_forced), col(CaPreFinalSchema.prelievo_annuo_prev)).cast(LongType).cast(StringType))
      .withColumn(CaPreFinalSchema.cod_prof_prel_std, coalesce(col(CaPreFinalSchema.cod_prof_prel_std_forced), col(CaPreFinalSchema.cod_prof_prel_std)))
      .withColumn(CaPreFinalSchema.cat_uso, coalesce(col(CaPreFinalSchema.cat_uso_forced), col(CaPreFinalSchema.cat_uso)))
      .withColumn(CaPreFinalSchema.classe_prelievo, coalesce(col(CaPreFinalSchema.classe_prelievo_forced), col(CaPreFinalSchema.classe_prelievo)))
      .withColumn(CaPreFinalSchema.zona_climatica, coalesce(col(CaPreFinalSchema.zona_climatica_forced), col(CaPreFinalSchema.zona_climatica)))
      .withColumn(CaPreFinalSchema.trattamento, coalesce(col(CaPreFinalSchema.trattamento_forced), col(CaPreFinalSchema.trattamento)))
      .withColumn(CaFinalSchema.piva_distr, lit(""))
      .withColumn(CaFinalSchema.piva_udd, lit(""))
      .withColumn(CaFinalSchema.piva_udb, lit(""))
      .selectExpr(CaFinalSchema.getValues: _*)
      .distinct()

    val udbRecPiva = udb
      .join(azienda, udb(RcuGasUdbPSchema.n_id_azienda) === azienda(RcuAziendaPSchema.n_id_azienda), "inner")
      .select(udb(RcuGasUdbPSchema.n_id_udb).alias("n_id_udb_to_drop"), azienda(RcuAziendaPSchema.t_piva))

    caFinal
      .drop(CaFinalSchema.piva_distr, CaFinalSchema.piva_udd, CaFinalSchema.piva_udb)
      .join(distributore, caFinal(CaFinalSchema.n_id_distr) === distributore(RcugasDistributoreSchema.n_id_distributore), "left")
      .drop(col(RcugasDistributoreSchema.n_id_distributore))
      .withColumnRenamed(RcugasDistributoreSchema.t_piva, CaFinalSchema.piva_distr)

      .join(broadcast(azienda), caFinal(CaFinalSchema.n_id_az_udd) === azienda(RcuAziendaPSchema.n_id_azienda), "left")
      .drop(col(RcuAziendaPSchema.n_id_azienda))
      .withColumnRenamed(RcuAziendaPSchema.t_piva, CaFinalSchema.piva_udd)

      .join(broadcast(udbRecPiva), caFinal(CaFinalSchema.n_id_udb) === udbRecPiva("n_id_udb_to_drop"), "left")
      .drop(col("n_id_udb_to_drop"))
      .withColumnRenamed(RcuAziendaPSchema.t_piva, CaFinalSchema.piva_udb)
  }

  def write(caFinal: DataFrame, mode: String = "append"): Unit = {
    caFinal.selectExpr(CaFinalSchema.getValues: _*)
      .write
      .mode(mode)
      .partitionBy(CaFinalSchema.session, CaFinalSchema.tipo_trasmissione, CaFinalSchema.anno_competenza, CaFinalSchema.executionid)
      .parquet(Environment.getCaFinalPath)

    if(!Environment.isLocalMode) Environment.getSpark.sql(s"MSCK REPAIR TABLE ${Environment.getCaFinalTableName}")
  }

  def writeToExport(caFinal: DataFrame): Unit = {
    caFinal.selectExpr(CaFinalExportSchema.getValues: _*)
      .write
      .mode("overwrite")
      .parquet(Environment.getCaFinalToExportPath)
  }
}
