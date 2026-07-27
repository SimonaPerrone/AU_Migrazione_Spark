create external table ${tsg.db}.${tsg.AtgTabProfiliGiornStdPercBkp.tableName}(
	  data string,
	  pprofk double,
	  pprofk_norm double,
	  id_reg_clim int,
	  prof string,
	  wkr double,
	  wkr_norm int)
partitioned by (annomese string, executionid bigint)
stored as parquet
location '${tsg.AtgTabProfiliGiornStdPercBkp.basepath}'