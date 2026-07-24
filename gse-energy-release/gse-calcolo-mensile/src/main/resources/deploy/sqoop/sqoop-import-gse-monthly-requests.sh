#!/bin/sh

export HADOOP_USER_NAME=${hadoop.username}

file_properties_path=${sqoop.config.path}

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

SQOOP_DATE=$(date +'%Y-%m-%d %H:%M:%S')
echo "sqoop date: $SQOOP_DATE"

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --columns "N_ID_GSE_RICHIESTA_ER_M,T_STATO,T_MESE_ANNO,T_COD_POD,T_CLIENTE,D_DATA_INSERIMENTO,D_DATA_MODIFICA" \
    --table "GSE.GSE_RICHIESTA_ER_M" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-import \
    --hive-database "${gse.db}" \
    --hive-table "GSE_RICHIESTA_ER_M" \
    --split-by "N_ID_GSE_RICHIESTA_ER_M" \
    --num-mappers 10 \
    --hive-overwrite \
    --map-column-hive N_ID_GSE_RICHIESTA_ER_M=bigint,T_STATO=string,T_MESE_ANNO=string,T_COD_POD=string,T_CLIENTE=string,D_DATA_INSERIMENTO=timestamp,D_DATA_MODIFICA=timestamp