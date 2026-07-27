

export HADOOP_USER_NAME=${hadoop_username}

file_properties_path="${sqoop.config.path}"

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table 'CA_FINAL' \
    --hcatalog-database '${hive.db}' \
    --hcatalog-table 'ca_final_to_export' \
    --staging-table 'CA_FINAL_STAGING' \
    --clear-staging-table \
    --num-mappers 20
