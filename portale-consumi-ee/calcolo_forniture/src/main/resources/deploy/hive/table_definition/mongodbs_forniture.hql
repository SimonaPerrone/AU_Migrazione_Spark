CREATE TABLE ${hive.db.mongodbs}.${hive.table.forniture} (
  n_id_fornitura STRING,
  inizio BIGINT,
  fine BIGINT,
  d_inizio_str STRING,
  d_fine_str STRING,
  codice_pod STRING,
  attivo STRING,
  n_id_pod STRING,
  n_id_fornitore STRING,
  t_tipo_mercato STRING,
  n_id_cliente STRING,
  n_id_indirizzo STRING,
  n_id_ind_forn STRING,
  t_servizio_tutela_sii STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);
