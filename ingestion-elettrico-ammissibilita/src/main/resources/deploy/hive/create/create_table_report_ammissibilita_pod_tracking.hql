CREATE TABLE ${hive.au}.${hive.table.report_ammissibilita_pod_tracking} (
  cartella_cloud string
  , report_filename string
  , input_filename string
  , cod_pod string
  , d_creazione_report timestamp
  , annomesegiornodir string
  )
PARTITIONED BY (
  annomese string)
STORED AS PARQUET
LOCATION "${hive.location}/${hive.table.report_ammissibilita_pod_tracking}"
