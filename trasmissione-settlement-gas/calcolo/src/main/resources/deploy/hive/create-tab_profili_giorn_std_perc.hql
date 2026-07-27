create external table ${tsg.db}.${tsg.TabProfiliGiornStdPerc.tableName}
(
      data string,
      pprofk double,
      pprofk_norm double,
      id_reg_clim int,
      prof string,
      wkr double,
      wkr_norm int,
      annotermico string
) partitioned by (executionid string)
stored as parquet
location '${tsg.TabProfiliGiornStdPerc.basepath}'