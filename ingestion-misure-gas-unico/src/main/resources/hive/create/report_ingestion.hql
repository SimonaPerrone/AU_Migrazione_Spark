CREATE TABLE ${hiveconf:hive_db}.report_ingestion (
  d_caricamento string
  , local_file string
  , flow_name string
  , t_anno_caricamento string
  , t_mese_caricamento string
  , t_giorno_caricamento string
  , piva_utente string
  , piva_distr string
  , t_name_file string
)
PARTITIONED BY (
  annomese string
)
STORED AS PARQUET;
