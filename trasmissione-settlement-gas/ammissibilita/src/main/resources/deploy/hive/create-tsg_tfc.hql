create external table ${tsg.db}.${tsg.TSGTFC.tableName}
(
      n_id_tsg2_file bigint,
      nome_file string,
      data string,
      id_reg_clim bigint,
      wkr double,
      verifica_amm boolean,
      cod_causale string,
      motivazione string
) partitioned by (annomese string, executionid bigint, progressivo string)
stored as parquet
location '${tsg.TSGTFC.basepath}'