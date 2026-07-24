#!/bin/bash
source <(hdfs dfs -cat "${connection.string.oracle}" \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

TABLEGSV5RICHIESTAORACLE=GSV.GSV5_CONS_RICHIESTA
TABLEGSV5RICHIESTA=GSV5_CONS_RICHIESTA
TABLEGSV5FORNITUREORACLE=GSV.GSV5_CONS_FORNITURE
TABLEGSV5FORNITURE=GSV5_CONS_FORNITURE

SQOOPDB=${hive.gsv}
PATHHDFSRICHIESTA=${hive.gsvrichiestalocation}
PATHHDFSFORNITURE=${hive.gsvforniturelocation}


hdfs dfs -rm  ${PATHHDFSRICHIESTA}/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLEGSV5RICHIESTAORACLE}" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLEGSV5RICHIESTA}" \
    --create-hive-table \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_GSV5_CONS_RICHIESTA" \
    --num-mappers 2 \
    --delete-target-dir \
    --target-dir "${PATHHDFSRICHIESTA}" \
    --map-column-hive N_ID_GSV5_CONS_RICHIESTA=string,T_TIPO=string,T_ANNO=string,D_DATA_RICHIESTA=string,T_STATO=string,D_DATA_INSERIMENTO=string,D_DATA_MODIFICA=string


hdfs dfs -rm  ${PATHHDFSFORNITURE}/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLEGSV5FORNITUREORACLE}" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLEGSV5FORNITURE}" \
    --create-hive-table \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_GSV5_CONS_FORNITURE" \
    --num-mappers 2 \
    --delete-target-dir \
    --target-dir "${PATHHDFSFORNITURE}" \
    --map-column-hive N_ID_GSV5_CONS_FORNITURE=string,N_ID_GSV5_CONS_RICHIESTA=string,N_ID_PDR=string,T_CODICE_PDR=string,T_COD_TIPO_PDR=string,T_CF_CLIENTE=string,T_PIVA_CLIENTE=string,T_RAG_SOC_CLIENTE=string,N_ID_CLIENTE=string,T_PIVA_DD=string,T_ANNO=string,D_DATA_INIZIO=string,D_DATA_FINE=string,FORN_CONTINUE=string,T_EXECUTION_ID=string,D_DATA_INSERIMENTO=string

if [ "$?" -ne 0 ];
then
    exit 1
fi