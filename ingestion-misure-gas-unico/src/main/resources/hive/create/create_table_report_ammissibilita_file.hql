create table ${hiveconf:hive_db}.report_ammissibilita_file_gas (
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