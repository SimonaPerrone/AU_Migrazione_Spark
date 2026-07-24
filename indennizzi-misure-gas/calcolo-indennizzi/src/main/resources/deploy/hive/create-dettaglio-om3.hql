create external table ${cig.db}.${dettaglioOM3.tableName}
(
    id_indennizzo bigint,
    piva_distr string,
    rag_soc_distr string,
    piva_udd string,
    rag_soc_udd string,
    percentage_lower_bound double,
    percentage_upper_bound double,
    target_percentage double,
    achieved_percentage double,
    pdr_base bigint,
    pdr_target double,
    delta_pdr_om2 double,
    pdr_count bigint,
    delta_pdr double,
    euro_fee_per_pdr double,
    indennizzo double
) partitioned by (annomese string, executionid bigint)
stored as parquet
location '${dettaglioOM3.basepath}'