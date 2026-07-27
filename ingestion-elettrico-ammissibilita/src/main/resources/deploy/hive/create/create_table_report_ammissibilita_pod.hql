CREATE TABLE  ${hive.au}.report_ammissibilita_pod (
  cartella_cloud string,
  nome_file string,
  pod string,
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
location "${hive.location}/report_ammissibilita_pod"