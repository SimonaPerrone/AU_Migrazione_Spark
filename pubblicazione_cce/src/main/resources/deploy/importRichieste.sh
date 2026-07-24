#!/bin/bash
# Import dati Richieste POD e FILTRO da Oracle per data richiesta in input
# ./importRichieste.sh 2024-01-01

set -e
REQUEST_DATE=$1

TS_NOW=$(date "+%Y-%m-%d %H:%M:%S")

oracle_table_pod=${oracle.table.cceRichiestaPod}
oracle_table_filtro=${oracle.table.cceRichiestaFiltro}

hive_db="${sqoop.db.import}"
hive_tabella_pod="${sqoop.table.cceRichiestaPod}"
hive_tabella_filtro="${sqoop.table.cceRichiestaFiltro}"

file_properties_path="${sqoop.config.path}"
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')


log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}


log "Inizio import richieste"

if [ -z "$1" ]
  then
    log "No request date specified. Using default:"
    REQUEST_DATE=$(date -d "$date -1 days" +"%Y-%m-%d")
fi

log "REQUEST_DATE: $REQUEST_DATE"

log "Pulizia tabelle richieste"

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -e "TRUNCATE TABLE ${hive_db}.${hive_tabella_pod}"

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -e "TRUNCATE TABLE ${hive_db}.${hive_tabella_filtro}"

log "Import richieste POD ${oracle_table_pod} in ${hive_db}.${hive_tabella_pod}"
sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --query "SELECT N_ID_RICHIESTA,T_SERVIZIO,T_PROCESSO,D_DATA_RICHIESTA,T_ANNO,T_MESE,T_RUOLO,T_PIVA,T_CODICE_POD,B_AMMISSIBILITA,T_COD_CAUSALE,T_MOTIVAZIONE,T_NOME_FILE,T_TIPO_AMM, '${TS_NOW}' AS SQOOP_DATE FROM CCE.CCE_RICHIESTA_POD WHERE TO_CHAR(D_DATA_RICHIESTA,'YYYY-MM-DD')='${REQUEST_DATE}' AND \$CONDITIONS" \
    --hcatalog-database $hive_db \
    --hcatalog-table $hive_tabella_pod \
    --hcatalog-partition-keys partition_request_date \
    --hcatalog-partition-values ${REQUEST_DATE} \
    --num-mappers 1


log "Import richieste FILTRO ${oracle_table_filtro} in ${hive_db}.${hive_tabella_filtro}"

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --query "SELECT N_ID_RICHIESTA,T_TIPO,T_SERVIZIO,T_PROCESSO,D_DATA_RICHIESTA,T_ANNO,T_MESE,T_RUOLO,T_PIVA,T_TENSIONE,T_ZONA,T_TIPO_POD,T_PIVA_UDD,T_PIVA_DD AS T_PIVA_ID,T_CODICE_TERNA,T_TARIFFA,'${TS_NOW}' AS SQOOP_DATE FROM CCE.CCE_RICHIESTA_FILTRO WHERE TO_CHAR(D_DATA_RICHIESTA,'YYYY-MM-DD')='${REQUEST_DATE}' AND \$CONDITIONS" \
    --hcatalog-database $hive_db \
    --hcatalog-table $hive_tabella_filtro \
    --hcatalog-partition-keys partition_request_date \
    --hcatalog-partition-values ${REQUEST_DATE} \
    --num-mappers 1

log "Fine import richieste"

exit 0
