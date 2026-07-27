CREATE TABLE ${hive.db.mongodbs}.${hive.table.gdm} (
  n_id_pod STRING,
  codice_pod STRING,
  n_potenza_disponibile STRING,
  n_potenza_impegnata STRING,
  n_tensione STRING,
  t_tipo_misuratore STRING,
  d_oper_misurator_att BIGINT,
  d_oper_misurator_att_str STRING,
  cambio_gdm STRING,
  data_cambio_gdm BIGINT,
  data_cambio_gdm_str STRING,
  trattamento STRING,
  stato_misuratore_2g STRING,
  t_mat_misuratore_att STRING,
  d_inst_misurator_att BIGINT,
  anno_start_misure_orarie INT,
  mese_start_misure_orarie INT
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);