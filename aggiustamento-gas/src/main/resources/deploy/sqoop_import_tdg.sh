#!/bin/bash
source <(hdfs dfs -cat "${connection.string.oracle}" \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

TABLETDG4COEFFKORACLE=TDG.TDG_TDG4_COEFFK
TABLETDG4COEFFK=TDG_TDG4_COEFFK

SQOOPDB=${hive.tdg}
PATHHDFS=${hive.tdglocation}

hdfs dfs -rm  ${hive.tdglocation}/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLETDG4COEFFKORACLE}" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLETDG4COEFFK}" \
    --create-hive-table \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_TDG4_COEFFK" \
    --num-mappers 2 \
    --delete-target-dir \
    --target-dir "${PATHHDFS}" \
    --map-column-hive N_ID_TDG4_COEFFK=int,N_ID_PDR=string,N_VAL_K=double,D_DATA_INIZIO=timestamp,D_DATA_FINE=timestamp,D_DATA_RIF=timestamp,T_TIPO_OP=string,D_DATA_INSERIMENTO=timestamp,D_DATA_AGGIORNAMENTO=timestamp

if [ "$?" -ne 0 ];
then
    exit 1
fi