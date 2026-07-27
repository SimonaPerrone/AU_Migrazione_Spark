create external table ${agg.db}.monthtreatment
(
    pdr string,
    month string,
    treatment string,
    calcmode string,
    autofilled boolean
) partitioned by (session string, executionid bigint)
stored as parquet
location '${agg.output.rootpath}/monthtreatment'
