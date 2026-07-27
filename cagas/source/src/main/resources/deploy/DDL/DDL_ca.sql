
CREATE EXTERNAL TABLE IF NOT EXISTS ${hive.db}.ca
(   pdr STRING,
    startService STRING,
    endService STRING,
    startSegment TIMESTAMP,
    endSegment TIMESTAMP,
    startValue DOUBLE,
    endValue DOUBLE,
    idConsumptionErrorState INT,
    caValue DOUBLE,
    idCaErrorCode INT,
    caMethods INT,
    codiceProfilo STRING,
    id_regClim INT,
    pprof_ce DOUBLE,
    pprofnk_wkr DOUBLE,
    t_comune_istat_pdr STRING,
    next_cod_profilo STRING,
    profMode INT,
    ca_sum BIGINT
) PARTITIONED BY(session STRING, executionid BIGINT)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${ca.basePath}'
;