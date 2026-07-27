DROP TABLE IF EXISTS ${hive.table.mid1_dettaglio};
CREATE TABLE ${hive.table.mid1_dettaglio} (
    pdr STRING,
    contatore INT,
    piva_id STRING,
    piva_udd STRING,
    cod_remi STRING,
    gdm STRING,
    alpha INT,
    executionid_mid_contatori BIGINT
)
PARTITIONED BY (annomese STRING, executionid BIGINT)
STORED AS PARQUET;
