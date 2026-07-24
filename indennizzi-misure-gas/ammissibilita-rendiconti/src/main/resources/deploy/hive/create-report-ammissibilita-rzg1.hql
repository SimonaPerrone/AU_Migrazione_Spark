create external table ${cig.db}.${reportAmmissibilita.tableName}
(
    cartella_cloud string,
    zip_file_name string,
    zip_last_modified_date bigint,
    csv_file_name string,
    cartella_cloud_ammissibilita string,
    ammissibilita_file_name string,
    ammissibilita boolean,
    codice string,
    descrizione string,
    data_creazione timestamp,
    executionid bigint
) partitioned by (annomese string)
stored as parquet
location '${cig.reportAmmissibilita.rzg1.basepath}'