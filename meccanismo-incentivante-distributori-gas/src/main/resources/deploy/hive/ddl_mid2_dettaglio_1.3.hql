CREATE TABLE ${hive.table.mid2_dettaglio}_bkp_v1 AS
SELECT * FROM ${hive.table.mid2_dettaglio};

DROP TABLE ${hive.table.mid2_dettaglio};
CREATE TABLE ${hive.table.mid2_dettaglio} (
    pdr STRING,
    contatore INT,
    piva_id STRING,
    rag_soc_id STRING,
    stato_id STRING,
    piva_udd STRING,
    rag_soc_udd STRING,
    piva_distr_att STRING,
    rag_soc_distr_att STRING,
    cod_remi STRING,
    gdm STRING,
    alpha INT,
    executionid_mid_contatori BIGINT
)
PARTITIONED BY (annomese STRING, executionid BIGINT)
STORED AS PARQUET;

