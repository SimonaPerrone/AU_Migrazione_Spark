#!/bin/sh

export HADOOP_USER_NAME=${hadoop.username}

file_properties_path=${sqoop.config.path}
errorMessage=0

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table 'GSE.GSE_AGGR_M' \
    --columns 'N_ID_GSE_RICHIESTA_ER_M,T_MESE_ANNO,T_COD_POD,N_CONSUMO_MENSILE,N_EXECUTION_ID,D_DATA_CREAZIONE' \
    --hcatalog-database '${gse.db}' \
    --hcatalog-table 'gse_aggr_m_export' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --staging-table 'GSE.GSE_AGGR_M_STG' \
    --clear-staging-table \
    --num-mappers 20

errorMessage=$?

# clear Hive staging table if the sqoop export finished successfully
if [[(${errorMessage} -eq 0)]];
then
  echo "Sqoop export success, delete GSE_AGGR_M_EXPORT data..."
  hdfs dfs \
      -rm \
      -f \
      -skipTrash \
      "${hdfs.output.path}/gse_aggr_m_export/*"
else
    exit $errorMessage
fi