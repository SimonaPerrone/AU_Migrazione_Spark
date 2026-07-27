create external table ${agg.db}.query_elenco_flussi_dettaglio_incoerenti_sbg
( pdr string,
  piva_distr string,
  piva_it string,
  piva_udd string,
  piva_udb string,
  piva_rdb string,
  service string,
  read_type string,
  motivation int,
  serial_number_mis string,
  serial_number_conv string,
  measure double,
  converted double,
  cau_int_mis int,
  cau_int_corr int,
  trattamento string,
  sessione string,
  nome_file string,
  dailyconsumption_executionid bigint,
  annomese string
) partitioned by (executionid bigint)
stored as parquet
location '${hdfs.basePath}/query_elenco_flussi_dettaglio_incoerenti'
