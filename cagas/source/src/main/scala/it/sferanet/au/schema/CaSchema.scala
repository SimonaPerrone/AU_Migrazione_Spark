package it.sferanet.au.schema

import org.apache.spark.sql.types._

object CaSchema extends SchemaEnum {
  val
  pdr,
  startService,
  endService,
  startSegment,
  endSegment,
  startValue,
  endValue,
  idConsumptionErrorState,
  caValue,
  idCaErrorCode,
  caMethods,
  codiceProfilo,
  id_regClim,
  pprof_ce,
  pprofnk_wkr,
  t_comune_istat_pdr,
  next_cod_profilo,
  profMode,
  ca_sum,

  start_local_file,
  end_local_file,
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

  executionid
  = Value

  val schema: StructType =
    StructType(
      StructField(pdr, StringType, nullable = false) ::
        StructField(startService, StringType, nullable = true) ::
        StructField(endService, StringType, nullable = true) ::
        StructField(startSegment, TimestampType, nullable = true) ::
        StructField(endSegment, TimestampType, nullable = true) ::
        StructField(startValue, DoubleType, nullable = true) ::
        StructField(endValue, DoubleType, nullable = true) ::
        StructField(idConsumptionErrorState, IntegerType, nullable = false) ::
        StructField(caValue, DoubleType, nullable = false) ::
        StructField(idCaErrorCode, IntegerType, nullable = false) ::
        StructField(caMethods, IntegerType, nullable = false) ::
        StructField(codiceProfilo, StringType, nullable = false) ::
        StructField(id_regClim, IntegerType, nullable = false) ::
        StructField(pprof_ce, DoubleType, nullable = false) ::
        StructField(pprofnk_wkr, DoubleType, nullable = false) ::
        StructField(t_comune_istat_pdr, StringType, nullable = false) ::
        StructField(next_cod_profilo, StringType, nullable = false) ::
        StructField(profMode, IntegerType, nullable = false) ::
        StructField(ca_sum, LongType, nullable = false) ::

        StructField(start_local_file, StringType, nullable = false) ::
        StructField(end_local_file, StringType, nullable = false) ::
        StructField(start_t_misuratore_integrato, StringType, nullable = false) ::
        StructField(end_t_misuratore_integrato, StringType, nullable = false) ::
        StructField(start_t_pre_conv, StringType, nullable = false) ::
        StructField(end_t_pre_conv, StringType, nullable = false) ::
        StructField(n_coeff_correzione, StringType, nullable = false) ::

        StructField(pres_tds, BooleanType, nullable = false) ::
        StructField(tipologia_uso, BooleanType, nullable = false) ::
        StructField(comp_termica, BooleanType, nullable = false) ::
        StructField(cat_uso_tds, StringType, nullable = true) ::
        StructField(classe_prelievo_tds, StringType, nullable = true) ::
        StructField(cod_istat_last_rcu, StringType, nullable = true) ::
        StructField(zona_climatica_lookup, StringType, nullable = true) ::
        Nil)
}
