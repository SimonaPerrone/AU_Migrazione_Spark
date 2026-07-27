CREATE TABLE ${hive.db.mongodbs}.${hive.table.forniture_info} (
  n_id_fornitura STRING,
  n_id_pod STRING,
  n_id_cliente STRING,
  d_inizio_titolarita BIGINT,
  d_fine_titolarita BIGINT,
  d_inizio_titolarita_str STRING,
  d_fine_titolarita_str STRING,
  n_id_fornitore STRING,
  t_tipo_mercato STRING,
  n_id_indirizzo STRING,
  n_id_ind_forn STRING,
  codice_pod STRING,
  t_residente STRING,
  t_tariffa_distr STRING,
  t_piva STRING,
  t_rag_soc STRING,
  t_servizio_tutela_sii STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);
