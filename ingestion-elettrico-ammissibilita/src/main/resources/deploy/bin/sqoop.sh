#!/bin/bash
source <(hdfs dfs -cat '/apps/deploy/job.properties' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

TABLERCUAZIENDAPORACLE=RCU.RCU_AZIENDA
TABLERCUDISTRORACLE=RCU.RCU_DISTR
TABLERCUEMTORACLE=RCU.RCU_EMT
TABLERCUPODDISTRORACLE=RCU.RCU_POD_DISTR
TABLERCUPODORACLE=RCU.RCU_POD
TABLERCUPODUDDORACLE=RCU.RCU_POD_UDD
TABLERCUSPODDISTRORACLE=RCU.RCUS_POD_DISTR
TABLERCUSPODORACLE=RCU.RCUS_POD
TABLERCUSPODUDDORACLE=RCU.RCUS_POD_UDD
TABLERCUSUDDORACLE=RCU.RCUS_UDD
TABLERCUUDDORACLE=RCU.RCU_UDD

TABLERCUAZIENDAP=RCU_AZIENDA_P
TABLERCUDISTR=RCU_DISTR_P
TABLERCUEMT=RCU_EMT_P
TABLERCUPODDISTR=RCU_POD_DISTR_P
TABLERCUPOD=RCU_POD_P
TABLERCUPODUDD=RCU_POD_UDD_P
TABLERCUUDD=RCU_UDD_P
TABLERCUSPODDISTR=RCUS_POD_DISTR_P
TABLERCUSPO=RCUS_POD_P
TABLERCUSPODUDD=RCUS_POD_UDD_P
TABLERCUSUDD=RCUS_UDD_P

SQOOPDB=${hive.rcu}
PATHHDFS=${hive.rculocation}

hdfs dfs -rm  {hive.sqoop.location}/rcu_azienda_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUAZIENDAPORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_AZIENDA" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUAZIENDAP}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_AZIENDA=string,N_ID_UTENTE=string,T_CODICE_AEEG=string,T_PIVA=string,T_CF=string,T_RAG_SOC=string,N_ID_SEDELEGALE=string,T_CONTATTO=string,T_EMAIL=string,T_PEC=string,D_AGGIORNAMENTO=string,N_ID_TRACCIA=string,N_ID_S_PREC=string

if [ "$?" -ne 0 ];
then
    exit 1
fi


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
    --warehouse-dir "${PATHHDFS}" \
    --map-column-hive N_ID_DISTR=string, T_CODICE_TERNA=string, T_TIPO=string, N_ID_DISTR_RIF=string, D_AGGIORNAMENTO=string, N_ID_TRACCIA=string, N_ID_S_PREC=string

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
    --map-column-hive N_ID_EMT=string,D_AGGIORNAMENTO=string,N_ID_TRACCIA=string,N_ID_S_PREC=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcu_pod_distr_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUPODDISTRORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_POD" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUPODDISTR}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_POD=string,N_ID_DISTR=string,D_INIZIO=string, D_FINE=string,T_NOTA=string, D_AGGIORNAMENTO=string, N_ID_TRACCIA=string, N_ID_S_PREC=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcu_pod_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUPODORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_POD" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUPOD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_POD=string,T_CODICE_POD=string,T_AREA_RIF=string, B_RICH_INDENNIZZO=string, B_RICH_PREST_DISTR=string, N_ID_INDIRIZZO=string, T_NOTA=string, D_AGGIORNAMENTO=string, N_ID_TRACCIA=string, N_ID_S_PREC=string, N_ID_IND_FORN=string

if [ "$?" -ne 0 ];
then
    exit 1
fi


hdfs dfs -rm  {hive.sqoop.location}/rcu_pod_udd_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUPODUDDORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_POD" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUPODUDD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_POD=string,N_ID_UDD=string,D_INIZIO=string,D_FINE=string,D_STIPULA=string,T_NOTA=string,D_AGGIORNAMENTO=string,N_ID_TRACCIA=string, N_ID_S_PREC=string

if [ "$?" -ne 0 ];
then
    exit 1
fi


hdfs dfs -rm  {hive.sqoop.location}/rcus_pod_distr_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUSPODDISTRORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_SCHEDA" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUSPODDISTR}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_SCHEDA=string,N_ID_POD=string,N_ID_DISTR=string,D_INIZIO=string,D_FINE=string,T_NOTA=string,D_AGGIORNAMENTO=string,D_ARCHIVIAZIONE=string,N_ID_TRACCIA=string,N_ID_S_PREC=string,N_ID_S_SUCC=string,B_VALIDO=string


if [ "$?" -ne 0 ];
then
    exit 1
fi


hdfs dfs -rm  {hive.sqoop.location}/rcus_pod_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUSPODORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_SCHEDA" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUSPOD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_SCHEDA=string,N_ID_POD=string,T_CODICE_POD=string,T_AREA_RIF=string,B_RICH_INDENNIZZO=string, B_RICH_PREST_DISTR=string,N_ID_INDIRIZZO=string,T_NOTA=string,D_AGGIORNAMENTO=string,D_ARCHIVIAZIONE=string,N_ID_TRACCIA=string,N_ID_S_PREC=string,N_ID_S_SUCC=string,B_VALIDO=string,N_ID_IND_FORN=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcus_pod_udd_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUSPODUDDORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_SCHEDA" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUSPODUDD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_SCHEDA=string,N_ID_POD=string,N_ID_UDD=string, D_INIZIO=string,D_FINE=string,D_STIPULA=string, T_NOTA=string,D_AGGIORNAMENTO=string,D_ARCHIVIAZIONE=string, N_ID_TRACCIA=string,N_ID_S_PREC=string,N_ID_S_SUCC=string,B_VALIDO=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcus_udd_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUSUDDORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_SCHEDA" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUSUDD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_SCHEDA=string,N_ID_UDD=string,T_CODICE_TERNA=string,D_AGGIORNAMENTO=string,D_ARCHIVIAZIONE=string,N_ID_TRACCIA=string,N_ID_S_PREC=string,N_ID_S_SUCC=string,B_VALIDO=string,D_INIZIO=string,D_FINE=string,N_ID_AZIENDA_RIF=string,T_TIPO=string



if [ "$?" -ne 0 ];
then
    exit 1
fi

hdfs dfs -rm  {hive.sqoop.location}/rcu_udd_p/*

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLERCUUDDORACLE}" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --split-by "N_ID_UDD" \
    --hive-import \
    --hive-database "${SQOOPDB}" \
    --hive-table "${TABLERCUUDD}" \
    --num-mappers 1 \
    --delete-target-dir \
    --warehouse-dir "${PATHHDFS}"\
    --map-column-hive N_ID_UDD=string,T_CODICE_TERNA=string,D_AGGIORNAMENTO=string,N_ID_TRACCIA=string,N_ID_S_PREC=string,D_INIZIO=string,D_FINE=string,N_ID_AZIENDA_RIF=string,T_TIPO=string

if [ "$?" -ne 0 ];
then
    exit 1
fi