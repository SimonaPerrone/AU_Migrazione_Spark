DROP TABLE IF EXISTS ${hive.table.cceEsitoExport};
CREATE EXTERNAL TABLE ${hive.table.cceEsitoExport}(
  `n_id_richiesta` STRING,
   `t_path` STRING,
   `t_file_esito` STRING,
   `t_file_ammissibilita` STRING,
   `t_stato` STRING,
   `d_data_esito` TIMESTAMP
   )
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'=',',
'line.delim'='\n',
'serialization.format'=',',
'serialization.null.format'='NULL')
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'hdfs://hdfscdp.fsisilon.siiau.local:8020/user/hive/warehouse/${sqoop.db.import}.db/cce_esito_export'
TBLPROPERTIES (
'TRANSLATED_TO_EXTERNAL'='TRUE',
'bucketing_version'='2',
'external.table.purge'='TRUE',
'spark.sql.create.version'='2.2 or prior');