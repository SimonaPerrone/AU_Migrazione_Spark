DROP TABLE IF EXISTS ${hive.table.misure_data_calcolo};
CREATE TABLE ${hive.table.misure_data_calcolo} (
    processo STRING, --3M, 33M
    data_calcolo STRING,
    ts_esecuzione TIMESTAMP
)
STORED AS PARQUET;
