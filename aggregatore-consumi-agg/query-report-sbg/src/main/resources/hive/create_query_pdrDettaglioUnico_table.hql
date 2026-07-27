create external table ${agg.db}.query_pdr_dettaglio_unico_sbg
( pdr string,
  piva_distr string,
  piva_it string,
  piva_udd string,
  piva_udb string,
  piva_rdb string,
  prelievo int,
  dtg string,
  cod_remi string,
  id_reg_clim int,
  cod_prof_prel_std string,
  trattamento string,
  tipo_cliente string,
  sessione string,
  dailyconsumption_executionid bigint,
  annomese string
) partitioned by (executionid bigint)
stored as parquet
location '${hdfs.basePath}/query_pdr_dettaglio_unico'
