create table ${hiveconf:hive_db}.report_ammissibilita_pdr (
  cartella_cloud string,
  nome_file string,
  flusso string,
  pdr string,
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