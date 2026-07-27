create external table ${agg.db}.query_esclusi_sbg
( pdr string,
  piva_distr string,
  piva_it string,
  piva_udd string,
  piva_udb string,
  piva_rdb string,
  dtg string,
  cod_remi string,
  prel_annuo_prev decimal(12,1),
  prelievo_aggregato int,
  id_reg_clim int,
  cod_prof_prel_std string,
  trattamento string,
  tipo_cliente string,
  sessione string,
  motivazione_esclusione string,
  dailyconsumption_executionid bigint,
  annomese string
) partitioned by (executionid bigint)
stored as parquet
location '${hdfs.basePath}/query_esclusi'