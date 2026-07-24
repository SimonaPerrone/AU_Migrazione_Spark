create external table ${cig.db}.${cig.reportPubblicazioneRzg2.tableName}
(
    input_table_execution_id string,
    operation_name string,
    base_name string,
    path_name string,
    load_date timestamp,
    annomese string
 ) partitioned by (executionid bigint)
 stored as parquet
 location '${cig.reportPubblicazioneRzg2.basepath}'