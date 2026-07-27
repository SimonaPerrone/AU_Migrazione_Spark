create external table ${tsg.db}.${tsg.PubblicazioneAmmissibilitaTFC.tableName}
(
      n_id_tsg2_file bigint,
      cartella_cloud string,
      csv_file_name string,
      last_modified bigint,
      ammissibilita_file_name string,
      data_amm timestamp,
      executionid bigint
) partitioned by (annomese string)
stored as parquet
location '${tsg.PubblicazioneAmmissibilitaTFC.basepath}'