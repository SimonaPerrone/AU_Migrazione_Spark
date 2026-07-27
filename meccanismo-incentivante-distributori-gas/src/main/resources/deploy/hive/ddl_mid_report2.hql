DROP TABLE IF EXISTS ${hive.table.mid_report2};
CREATE TABLE ${hive.table.mid_report2} (
    pdr STRING,
    annomese_sterilizzazione STRING,
    anno_sterilizzazione STRING,
    trattamento STRING,
    n INT,
    sterilizzato_exc STRING,
    sterilizzato_gdm STRING,
    sessione STRING,
    piva_dd STRING,
    cod_remi STRING,
    gdm STRING,
    rag_soc_dd STRING
)
STORED AS PARQUET;
