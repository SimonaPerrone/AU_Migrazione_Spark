DROP TABLE IF EXISTS ${hive.table.misure.consultazione};
CREATE TABLE ${hive.table.misure.consultazione} (
      cf_piva STRING,
      pod STRING,
      data_lettura STRING,
      data_ricezione STRING,
      motivazione STRING,
      lettura_monoraria DOUBLE,
      lettura_f1 DOUBLE,
      lettura_f2 DOUBLE,
      lettura_f3 DOUBLE,
      lettura_f4 DOUBLE,
      lettura_f5 DOUBLE,
      lettura_f6 DOUBLE,
      tipo_flusso STRING,
      ea STRING,
      er STRING,
      erc STRING,
      eri STRING
)
PARTITIONED BY (
    annomese_riferimento INT,
    cod_pod CHAR(2)
)
STORED AS PARQUET;
