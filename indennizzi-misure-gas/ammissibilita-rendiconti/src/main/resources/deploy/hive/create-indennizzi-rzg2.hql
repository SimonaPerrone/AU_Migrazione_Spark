create external table ${cig.db}.${indennizziRzg2.tableName}
(
    zip_file_name string,
    piva_utente string,
    piva_id string,
    piva_udd string,
    year_dir string,
    month_dir string,
    anno_mese_competenza string,
    zip_timestamp string,
    progressivo string,
    csv_file_name string,
    csv_data string,
    csv_id_indennizzo bigint,
    csv_piva_id string,
    csv_rag_soc_id string,
    csv_piva_udd string,
    csv_rag_soc_udd string,
    csv_euro_om1 string,
    csv_euro_om2 string,
    csv_euro_om3 string,
    euro_sii_om1 double,
    euro_sii_om2 double,
    euro_sii_om3 double,
    delta_om1 double,
    delta_om2 double,
    delta_om3 double,
    ammissibilita boolean,
    status_code string,
    status_message string
) partitioned by (executionid bigint)
stored as parquet
location '${cig.indennizziRzg2.basepath}'