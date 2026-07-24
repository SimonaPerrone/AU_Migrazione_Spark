#!/bin/bash
source <(hdfs dfs -cat '/apps/deploy/job.properties' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

TABLERCUDISTRORACLE=RCU.RCU_DISTR
TABLERCUEMTORACLE=RCU.RCU_EMT
TABLERCUDISTR=RCU_DISTR_P
TABLERCUEMT=RCU_EMT_P
SQOOPDB=${hive.rcu}
PATHHDFS=${hive.rculocation}

hdfs dfs -rm  {hive.sqoop.location}/rcu_distr_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUDISTRORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_DISTR" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUDISTR}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_DISTR=string,N_ID_DISTR_RIF=string,N_ID_TRACCIA=string,N_ID_S_PREC=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcu_emt_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUEMTORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_EMT" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUEMT}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}" \
    --map-column-hive N_ID_EMT=string,N_ID_TRACCIA=string,N_ID_S_PREC=string

if [ "$?" -ne 0 ];
then
    exit 1
fi