package it.sferanet.au.schema

import org.apache.spark.sql.types._

object CaFinalExportSchema extends SchemaEnum {
  val
  id_sag_ann,
  anno_competenza,
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
  tipo_trasmissione,
  codIstat,
  executionid

  = Value

  val schema: StructType =
    StructType(
      StructField(id_sag_ann, LongType) ::
        StructField(anno_competenza, StringType) ::
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
        StructField(d_ricezione, DateType) ::
        StructField(tipo_trasmissione, StringType) ::
        StructField(executionid, StringType) ::
        Nil)
}