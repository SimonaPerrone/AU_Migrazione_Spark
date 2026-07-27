create external table ${tsg.db}.${tsg.VPGFile.tableName}
(
      n_id_tsg2_file bigint,
      piva_rdb string,
      annotermico string,
      data_creazione string,
      nome_file string,
      progressivo string
) partitioned by (executionid bigint)
stored as parquet
location '${tsg.VPGFile.basepath}'