CREATE TABLE  ${hive.au}.${hive.table.report_ammissibilita_file} (
  cartella_cloud string,
  nome_file string,
  flusso string,
  ammissibilita string,
  bloccante string,
  codice_inamissibilita string,
  descrizione string,
  d_caricamento timestamp
) partitioned by (
  anno string,
  mese string,
  giorno string
) stored as parquet
location "${hive.location}/${hive.table.report_ammissibilita_file}"