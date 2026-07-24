create external table ${cig.db}.${pdrTotale.tableName}
(
    codice_pdr string,
    piva_distr string,
    rag_soc_distr string,
    piva_udd string,
    rag_soc_udd string,
    data_attivazione_pdr timestamp,
    local_file string,
    classe_gdm string,
    id_indennizzo bigint,
    nome_file_tgl_om1 string,
    nome_file_tgl_om2 string,
    nome_file_tgl_om3 string,
    cartella_cloud_tgl_om1 string,
    cartella_cloud_tgl_om2 string,
    cartella_cloud_tgl_om3 string,
    count_tgl_effettive bigint,
    count_tgl_stimate bigint
) partitioned by (annomese string, executionid bigint)
stored as parquet
location '${pdrTotale.basepath}'