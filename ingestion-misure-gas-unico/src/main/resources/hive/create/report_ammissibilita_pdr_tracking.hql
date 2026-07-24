CREATE TABLE ${hiveconf:hive_db}.report_ammissibilita_pdr_tracking (
  cartella_cloud string
  , report_filename string
  , input_filename string
  , cod_pdr string
  , d_creazione_report timestamp
  , annomesegiornodir string
  )
PARTITIONED BY (
  annomese string)
STORED AS PARQUET
