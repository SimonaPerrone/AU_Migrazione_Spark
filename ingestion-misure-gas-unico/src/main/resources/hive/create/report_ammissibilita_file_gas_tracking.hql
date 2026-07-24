CREATE TABLE ${hiveconf:hive_db}.report_ammissibilita_file_gas_tracking (
  cartella_cloud string
  , report_filename string
  , input_filename string
  , d_creazione_report timestamp
  , annomesegiornodir string
  )
PARTITIONED BY (
  annomese string)
STORED AS PARQUET
