create external table ${output.db}.aggregatore_info_log_clg
(  anno_mese string,
   base_name string,
   path_name string,
   load_date timestamp
) partitioned by (partition_date bigint)
stored as parquet
location '${hdfs.output.basepath.infoLog}'
