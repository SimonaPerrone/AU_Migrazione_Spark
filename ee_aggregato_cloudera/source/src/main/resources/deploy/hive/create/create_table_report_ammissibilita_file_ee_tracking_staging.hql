CREATE TABLE ${hive.au}.${hive.table.report_ammissibilita_file_ee_tracking_staging} (
  cartella_cloud string
  , report_filename string
  , input_filename string
  , d_creazione_report timestamp
  , annomesegiornodir string
  , annomese string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';