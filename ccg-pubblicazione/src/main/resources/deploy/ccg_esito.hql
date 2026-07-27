create external table ${ccg.db}.ccg_esito
(
 N_ID_RICHIESTA bigint,
 T_PATH string,
 T_FILE_ESITO string,
 T_FILE_AMMISSIBILITA string,
 T_STATO string,
 T_OPERATION_NAME string,
 T_NUMBER_FILE_ZIP int,
 EXECUTION_ID_INPUT_READ string,
 D_DATA_ESITO timestamp,
 TIPO_RICHIESTA string
) partitioned by (D_DATA_RICHIESTA string, SESSIONE string, executionid bigint)
 stored as parquet
 location '${table.path.hdfs}/ccg_esito'