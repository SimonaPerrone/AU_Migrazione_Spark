CREATE EXTERNAL TABLE IF NOT EXISTS ${hive.db}.consumptions
(
    pdr String,
    startService String,
    endService String,
    startSegment Timestamp,
    endSegment Timestamp,
    startvalue Double,
    endvalue Double,
    idConsumptionErrorState Int,
    n_coeff_correzione Double,
    t_misuratore_integrato String,
    end_t_misuratore_integrato String,
    t_pre_conv String,
    end_t_pre_conv String,
    t_cod_prof String,
    n_prelievo_annuo String,
    tipo_coeff String,
    tipo_forzatura String,
    coerenza_dim String
)
PARTITIONED BY(session STRING, executionid BIGINT)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${consumption.basepath}'
;