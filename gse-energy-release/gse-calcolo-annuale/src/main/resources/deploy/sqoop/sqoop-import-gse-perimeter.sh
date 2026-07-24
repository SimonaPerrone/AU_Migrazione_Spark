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
    --columns "N_ID_GSE_PERIMETRO_ER_EE,N_ID_GSE_RICHIESTA_ER_IMPORT,T_COD_POD,T_MESE_ANNO,T_CF_CLIENTE,T_PIVA_CLIENTE,T_VALIDO" \
    --table "GSE.GSE_PERIMETRO_ER_EE" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --delete-target-dir \
    --hive-import \
    --hive-database "${gse.db}" \
    --hive-table "GSE_PERIMETRO_ER_EE" \
    --split-by "N_ID_GSE_PERIMETRO_ER_EE" \
    --num-mappers 10 \
    --hive-overwrite \
    --map-column-hive N_ID_GSE_PERIMETRO_ER_EE=bigint,N_ID_GSE_RICHIESTA_ER_IMPORT=bigint,T_COD_POD=string,T_MESE_ANNO=string,T_CF_CLIENTE=string,T_PIVA_CLIENTE=string,T_VALIDO=string
