CREATE TABLE ${hive.db.mongodbs}.${hive.table.switch} (
  t_codice_pod STRING,
  data_switch BIGINT,
  n_id_pratica STRING,
  switching_in_corso STRING,
  n_id_cliente STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);
