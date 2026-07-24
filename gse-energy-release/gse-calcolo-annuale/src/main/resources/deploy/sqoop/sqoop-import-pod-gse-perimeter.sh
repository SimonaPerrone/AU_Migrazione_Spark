#!/bin/sh
# Esegue uno sqoop import dalla tabella GSE.GSE_PERIMETRO_ER_EE per la sola colonna (DISTINCT) T_COD_POD filtrando per ANNO
# e salva nella location che è letta dalla tabella GSE_BI.POD_ONE
# Esecuzione:
# ./sqoop-import-pod-gse-perimeter.sh ANNO

export HADOOP_USER_NAME=${hadoop.username}

ANNO=$1

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
    --query "SELECT DISTINCT T_COD_POD FROM GSE.GSE_PERIMETRO_ER_EE WHERE T_ANNO=${ANNO} AND T_VALIDO = 'Y' AND \$CONDITIONS" \
    --split-by T_COD_POD \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --delete-target-dir \
    --target-dir /user/hive/warehouse/gse_bi.db/pod_one
