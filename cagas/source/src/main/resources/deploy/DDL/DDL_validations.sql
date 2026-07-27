CREATE EXTERNAL TABLE IF NOT EXISTS ${hive.db}.validated_flows
(
    service String,
    pdr String,
    dat Timestamp,
    measure Double,
    converted Double,
    readType String,
    serialNumberMis String,
    serialNumberConv String,
    timestampLocalFile Timestamp,
    d_caricamento Int,
    local_file String,
    cat_uso String,
    classe_prelievo String,
    data_creazione Timestamp,
    motivazione_rettifica Int,
    cau_int_mis Int,
    cau_int_cor Int,
    file_rettifica String,
    n_coeff_correzione Double
)
PARTITIONED BY(session STRING, executionid BIGINT)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${validation.basepath}'
;