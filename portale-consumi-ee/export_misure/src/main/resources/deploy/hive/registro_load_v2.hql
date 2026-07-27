DROP TABLE IF EXISTS ${hive.table.misure.registro_load};
CREATE TABLE ${hive.table.misure.registro_load} (
  note STRING,
  numero_documenti BIGINT,
  last_run BIGINT
)
PARTITIONED BY (competenza_consumi INT,id_run STRING)
STORED AS PARQUET;

