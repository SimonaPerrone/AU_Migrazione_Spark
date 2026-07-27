create external table ${agg.db}.monthtreatment_sbg_ccg
(
    pdr string,
    month string,
    treatment string,
    calcmode string,
    autofilled boolean
) partitioned by (session string, executionid bigint)
stored as parquet
location '${agg.output.rootpath}/monthtreatment_sbg_ccg'
