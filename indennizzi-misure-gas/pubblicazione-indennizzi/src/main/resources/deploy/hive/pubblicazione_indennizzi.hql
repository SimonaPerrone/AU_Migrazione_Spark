create external table ${cig.db}.${cig.pubblicazioneIndennizzi.tableName} (
    input_table_execution_id string,
    operation_name string,
    base_name string,
    path_name string,
    load_date timestamp
) partitioned by (annomese string, executionid bigint)
stored as parquet
location '${cig.pubblicazioneIndennizzi.basepath}'