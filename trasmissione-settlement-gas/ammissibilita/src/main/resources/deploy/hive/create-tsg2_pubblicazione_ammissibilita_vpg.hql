create external table ${tsg.db}.${tsg.PubblicazioneAmmissibilitaVPG.tableName}
(
      n_id_tsg2_file bigint,
      cartella_cloud string,
      csv_file_name string,
      last_modified bigint,
      ammissibilita_file_name string,
      data_amm timestamp,
      executionid bigint
) partitioned by (annotermico string)
stored as parquet
location '${tsg.PubblicazioneAmmissibilitaVPG.basepath}'