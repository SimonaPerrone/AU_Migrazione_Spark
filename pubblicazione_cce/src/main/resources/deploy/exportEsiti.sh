#!/bin/bash
# Export dati esito da Hive a Oracle
# ./exportEsiti.sh
#

set -e

export HADOOP_USER_NAME=${hadoop_username}

hive_db="${sqoop.db.import}"
hive_table_esito="${sqoop.table.cceEsito}"
hive_export_dir="/user/hive/warehouse/${sqoop.db.import}.db/cce_esito_export"

oracle_table_esito="${oracle.table.cceEsito}"
oracle_staging_esito="${oracle.table.cceEsitoStaging}"

file_properties_path="${sqoop.config.path}"
JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')


log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}


log "Inizio export esiti"

log "Export esiti ${hive_db}.${hive_table_esito} in ${oracle_table_esito}"
sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${oracle_table_esito}" \
    --columns "N_ID_RICHIESTA,T_PATH,T_FILE_ESITO,T_FILE_AMMISSIBILITA,T_STATO,D_DATA_ESITO" \
    --export-dir "${hive_export_dir}" \
    --map-column-java "T_FILE_ESITO=String" \
    --input-fields-terminated-by ',' \
    --input-lines-terminated-by '\n' \
    --input-null-string 'NULL' \
    --input-null-non-string 'NULL' \
    --num-mappers 1

log "Fine export esiti"

exit 0
