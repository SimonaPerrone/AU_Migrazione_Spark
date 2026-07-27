DROP TABLE IF EXISTS ${hive.table.mid_report1};
CREATE TABLE ${hive.table.mid_report1} (
    pdr STRING,
    annomese STRING,
    trattamento STRING,
    n INT,
    piva_dd STRING,
    cod_remi STRING,
    gdm STRING,
    rag_soc_dd STRING
)
STORED AS PARQUET;
