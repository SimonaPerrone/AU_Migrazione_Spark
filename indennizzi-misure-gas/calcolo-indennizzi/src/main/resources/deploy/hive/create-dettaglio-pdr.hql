create external table ${cig.db}.${dettaglioPdr.tableName}
(
    id_indennizzo bigint,
    piva_id string,
    rag_soc_id string,
    piva_udd string,
    rag_soc_udd string,
    pdr string,
    nome_file string
) partitioned by (annomese string, executionid bigint)
stored as parquet
location '${dettaglioPdr.basepath}'