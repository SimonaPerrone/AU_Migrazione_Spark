CREATE TABLE ${hiveconf:hive_db}.report_ammissibilita_pdr_tracking_staging (
  cartella_cloud string
  , report_filename string
  , input_filename string
  , cod_pdr string
  , d_creazione_report timestamp
  , annomesegiornodir string
  , annomese string
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
