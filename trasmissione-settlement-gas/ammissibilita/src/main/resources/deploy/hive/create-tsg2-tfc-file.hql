create external table ${tsg.db}.${tsg.TFCFile.tableName}
(
      n_id_tsg2_file bigint,
      nome_file string,
      piva_rdb string,
      annomese string,
      data_creazione string,
      progressivo string
) partitioned by (executionid bigint)
stored as parquet
location '${tsg.TFCFile.basepath}'