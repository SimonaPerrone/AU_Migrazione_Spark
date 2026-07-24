DROP TABLE IF EXISTS ${hive.table.cceRichiestaFiltro};
CREATE TABLE ${hive.table.cceRichiestaFiltro} (
  n_id_richiesta STRING,
  t_tipo STRING,
  t_servizio STRING,
  t_processo STRING,
  d_data_richiesta STRING,
  t_anno STRING,
  t_mese STRING,
  t_ruolo STRING,
  t_piva STRING,
  t_tensione STRING,
  t_zona STRING,
  t_tipo_pod STRING,
  t_piva_udd STRING,
  t_piva_id STRING,
  t_codice_terna STRING,
  t_tariffa STRING,
  sqoop_date STRING
)
PARTITIONED BY (partition_request_date STRING)
STORED AS PARQUET;
