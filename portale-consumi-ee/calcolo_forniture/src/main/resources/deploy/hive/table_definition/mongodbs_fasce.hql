CREATE TABLE ${hive.db.mongodbs}.${hive.table.fasce} (
  n_id_pod STRING,
  n_id_misuratore STRING,
  f_lunedi STRING,
  f_martedi STRING,
  f_mercoledi STRING,
  f_giovedi STRING,
  f_venerdi STRING,
  f_sabato STRING,
  f_domenica STRING,
  f_festivo STRING,
  d_inizio_validita BIGINT,
  d_fine_validita BIGINT,
  d_fine_validita_str STRING,
  d_data_iniziofreezing STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);
