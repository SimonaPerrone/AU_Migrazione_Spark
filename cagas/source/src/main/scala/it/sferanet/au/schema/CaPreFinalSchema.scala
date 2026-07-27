package it.sferanet.au.schema

import org.apache.spark.sql.types._

object CaPreFinalSchema extends SchemaEnum {
  val
  id_sag_ann,
  n_id_distr,
  n_id_az_udd,
  n_id_udb,
  codice_remi,
  codice_pdr,
  cap_trasp_pdr,
  cat_uso,
  classe_prelievo,
  zona_climatica,
  id_reg_clim,
  cod_prof_prel_std,
  prelievo_annuo_prev,
  trattamento,
  d_ricezione,
  codistat,
  id_ca_error_code,
  start_local_file,
  end_local_file,
  calcmode,
  start_t_misuratore_integrato,
  end_t_misuratore_integrato,
  start_t_pre_conv,
  end_t_pre_conv,
  n_coeff_correzione,
  pres_tds,
  tipologia_uso,
  comp_termica,
  cat_uso_tds,
  classe_prelievo_tds,
  cod_istat_last_rcu,
  zona_climatica_lookup,
  prelievo_annuo_prev_forced,
  cod_prof_prel_std_forced,
  cat_uso_forced,
  zona_climatica_forced,
  classe_prelievo_forced,
  is_ca_calculated,
  startSegment,
  endSegment,
  trattamento_forced,
  massivo_freeze_executionid,
  freeze_date,
  session,
  tipo_trasmissione,
  anno_competenza,
  executionid
  = Value

  val schema: StructType =
    StructType(
      StructField(id_sag_ann, LongType) ::
        StructField(n_id_distr, LongType) ::
        StructField(n_id_az_udd, LongType) ::
        StructField(n_id_udb, LongType) ::
        StructField(codice_remi, StringType) ::
        StructField(codice_pdr, StringType) ::
        StructField(cap_trasp_pdr, StringType) ::
        StructField(cat_uso, StringType) ::
        StructField(classe_prelievo, StringType) ::
        StructField(zona_climatica, StringType) ::
        StructField(id_reg_clim, StringType) ::
        StructField(cod_prof_prel_std, StringType) ::
        StructField(prelievo_annuo_prev, StringType) ::
        StructField(trattamento, StringType) ::
        StructField(d_ricezione, StringType) ::
        StructField(tipo_trasmissione, StringType) ::
        StructField(codistat, StringType) ::
        StructField(id_ca_error_code, IntegerType) ::
        StructField(start_local_file, StringType) ::
        StructField(end_local_file, StringType) ::
        StructField(calcmode, StringType) ::
        StructField(start_t_misuratore_integrato, StringType) ::
        StructField(end_t_misuratore_integrato, StringType) ::
        StructField(start_t_pre_conv, StringType) ::
        StructField(end_t_pre_conv, StringType) ::
        StructField(n_coeff_correzione, StringType) ::
        StructField(pres_tds, BooleanType) ::
        StructField(tipologia_uso, BooleanType) ::
        StructField(comp_termica, BooleanType) ::
        StructField(cat_uso_tds, StringType) ::
        StructField(classe_prelievo_tds, StringType) ::
        StructField(cod_istat_last_rcu, StringType) ::
        StructField(zona_climatica_lookup, StringType) ::
        StructField(prelievo_annuo_prev_forced, StringType) ::
        StructField(cod_prof_prel_std_forced, StringType) ::
        StructField(cat_uso_forced, StringType) ::
        StructField(zona_climatica_forced, StringType) ::
        StructField(classe_prelievo_forced, StringType) ::
        StructField(is_ca_calculated, BooleanType) ::
        StructField(startSegment, TimestampType) ::
        StructField(endSegment, TimestampType) ::
        StructField(trattamento_forced, StringType) ::
        StructField(massivo_freeze_executionid, LongType) ::
        StructField(freeze_date, TimestampType) ::
        StructField(anno_competenza, StringType) ::
        StructField(executionid, LongType) ::
        Nil)
}