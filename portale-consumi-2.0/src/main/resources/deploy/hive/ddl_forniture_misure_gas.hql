DROP TABLE IF EXISTS ${hive.table.forniture_misure_gas};
CREATE TABLE ${hive.table.forniture_misure_gas} (
    codice_fiscale STRING,
    codice_pdr STRING,
    codice_fornitura STRING,
    lettura INT,
    flusso STRING,
    gruppo_flusso SMALLINT,
    data_lettura TIMESTAMP,
    data_caricamento TIMESTAMP,
    motivazione SMALLINT,
    delta_misure INT,
    usata_per_calcolo SMALLINT,
    riempimento INT,
    cod_pdr STRING
)
PARTITIONED BY (data_calcolo STRING, annomese STRING)
STORED AS PARQUET;
