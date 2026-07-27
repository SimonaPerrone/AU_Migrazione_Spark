SET hive.exec.dynamic.partition=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
WITH output_data as
(SELECT pod14,d_caricamento,nome_flusso,d_data_decorrenza,d_creazione
FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_OUTPUT_TABLE_NAME}
WHERE(1=1 and ANNOMESE-FILTER and d_caricamento = '${hiveconf:loading_timestamp}')
)
INSERT INTO TABLE ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_SCARTI_STORICI_OUTPUT_TABLE_NAME} PARTITION (annomese_sw)
SELECT DISTINCT
       bb.pod14,
       bb.d_data_decorrenza,
       from_unixtime(unix_timestamp('${hiveconf:loading_timestamp}','yyyy-MM-dd HH:mm:ss')) as d_caricamento,
       nome_flusso,
       bb.dp,
       bb.annomese_sw
FROM ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb
LEFT JOIN output_data on output_data.pod14=bb.pod14 and output_data.d_data_decorrenza=bb.d_data_decorrenza and output_data.d_creazione=bb.d_creazione
WHERE output_data.d_caricamento is null or output_data.nome_flusso='XXX' or bb.dp is null;