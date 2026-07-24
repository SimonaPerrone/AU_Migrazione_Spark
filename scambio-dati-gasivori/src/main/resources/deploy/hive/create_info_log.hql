create external table ${gasivori.db}.${aggregatore.infoLog.tableName}
(  input_table_filtering string,
   operation_name string,
   base_name string,
   path_name string,
   load_date timestamp
) partitioned by (partition_date bigint)
stored as parquet
location '${hdfs.infoLog.basepath}'
