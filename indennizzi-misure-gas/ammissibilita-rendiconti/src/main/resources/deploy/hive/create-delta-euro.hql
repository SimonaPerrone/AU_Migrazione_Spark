create external table ${cig.db}.${deltaEuro.tableName}
(
      id_indennizzo bigint,
      piva_distr string,
      rag_soc_distr string,
      piva_udd string,
      rag_soc_udd string,
      euro_sii_om1 double,
      euro_sii_om2 double,
      euro_sii_om3 double,
      euro_dd_om1 double,
      euro_dd_om2 double,
      euro_dd_om3 double,
      delta_om1 double,
      delta_om2 double,
      delta_om3 double,
      annomese string
) partitioned by (executionid bigint)
stored as parquet
location '${cig.deltaEuro.basepath}'