DROP TABLE IF EXISTS ${hive.table.cceEsito};
CREATE TABLE ${hive.table.cceEsito} (
    n_id_richiesta STRING,
    t_ruolo STRING,
    t_path STRING,
    t_file_esito STRING,
    t_file_ammissibilita STRING,
    t_stato STRING,
    t_operation_name STRING,
    t_number_file_zip INTEGER,
    execution_id_input_read STRING,
    d_data_esito TIMESTAMP,
    tipo_richiesta STRING,
    n_executionid STRING,
    d_data_richiesta STRING
)
STORED AS PARQUET;
