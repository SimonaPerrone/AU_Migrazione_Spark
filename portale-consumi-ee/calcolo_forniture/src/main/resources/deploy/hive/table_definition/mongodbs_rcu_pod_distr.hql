CREATE TABLE ${hive.db.mongodbs}.${hive.table.rcu_pod_distr} (
  n_id_pod STRING,
  t_rag_soc STRING
)
STORED AS PARQUET
TBLPROPERTIES (
  'bucketing_version' = '2'
);

