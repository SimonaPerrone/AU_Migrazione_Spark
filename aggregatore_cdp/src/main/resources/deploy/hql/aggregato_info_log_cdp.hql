create external table ${cdp.db}.aggregatore_info_log_cdp
(  daily_consumption_execution_id string,
   operation_name string,
   base_name string,
   path_name string,
   load_date timestamp
) partitioned by (partition_date bigint)
stored as parquet
location '${hdfs.output.basepath.infoLog}'
