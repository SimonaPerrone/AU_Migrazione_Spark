export HADOOP_USER_NAME=${hadoop.username}

file_properties_path=${sqoop.config.path}

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "CCG.CCG_ESITO" \
    --columns 'n_id_richiesta,t_path,t_file_esito,t_file_ammissibilita,t_stato,d_data_esito' \
    --hcatalog-database "${ccg.db}" \
    --hcatalog-table 'CCG_ESITO_EXPORT' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --staging-table 'CCG.CCG_ESITO_STG' \
    --clear-staging-table \
    --num-mappers 20 \
    --map-column-java n_id_richiesta=Integer,t_path=String,t_file_esito=String,t_file_ammissibilita=String,t_stato=String,d_data_esito=java.sql.Date


if [ "$?" -eq 0 ];
then
  echo "Sqoop export success, delete CCG_ESITO_EXPORT data..."
  # clear Hive staging table if the sqoop export finished successfully
  hdfs dfs \
      -rm \
      -f \
      -skipTrash \
      "${table.path.hdfs}/ccg_esito_export/*"
else
  echo "Sqoop export exited with error"
fi