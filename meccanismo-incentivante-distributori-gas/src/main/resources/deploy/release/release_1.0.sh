#!/bin/bash
###
# Rilascio applicativo versione 1.0
###
set -e

DATA_OGGI=$(date "+%Y%m%d%H%M%S")

LOCAL_PATH=${deploy.path.local}
HDFS_PATH=${deploy.path.hdfs}
HDFS_PATH_PATH_PROPERTIES=${deploy.path.properties}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"
PATH_PROPERTIES="${LOCAL_PATH}/params"

PATH_INCLUSIONI="${LOCAL_PATH}/file/calcolo/inclusioni.csv"
PATH_ESCLUSIONI="${LOCAL_PATH}/file/calcolo/esclusioni.csv"

PATH_ESCLUSIONI_PUBBLICAZIONE="${LOCAL_PATH}/file/pubblicazione"
PATH_VALORI_ALPHA="${PATH_ESCLUSIONI_PUBBLICAZIONE}/valori_alpha.csv"

LOGFILE=${deploy.path.logs}/${DATA_OGGI}-MID-rilascio_v.1.0.txt

log() {
    LOG_TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${LOG_TIMESTAMP}] - ${1}"
}

###

echo "Creazione cartella logs"
mkdir -p ${deploy.path.logs}

log "Inizio processo rilascio" | tee -a ${LOGFILE}

log "Impostazione permessi file" | tee -a ${LOGFILE}
chmod 755 ${LOCAL_PATH}/*

log "Creazione strutture Hive" | tee -a ${LOGFILE}
# calcolo
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid_contatori.hql

# preparazione mid1 e mid2
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid1_dettaglio.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid2_dettaglio.hql

# pubblicazione
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid_aggregatore_info.hql

# reportistica
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid_report1.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
  -n ${system.user} \
  -f ${PATH_HIVE_STRUCT}/ddl_mid_report2.hql

log "Creazione cartelle HDFS" | tee -a ${LOGFILE}
hdfs dfs -mkdir -p ${HDFS_PATH}
hdfs dfs -mkdir -p ${HDFS_PATH_PATH_PROPERTIES}

log "Caricamento file properties" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_PROPERTIES}/* ${HDFS_PATH}

log "Caricamento file inclusioni" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_INCLUSIONI} ${file.path.inclusioni_sbg}
hdfs dfs -put -f ${PATH_INCLUSIONI} ${file.path.inclusioni_agg}

log "Caricamento file esclusioni" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_ESCLUSIONI} ${file.path.esclusioni_sbg}
hdfs dfs -put -f ${PATH_ESCLUSIONI} ${file.path.esclusioni_agg}

log "Caricamento file esclusioni predispozione MID1" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_AM_list.csv ${file.path.mid1_esclusioni_annomese}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_DISTR_list.csv ${file.path.mid1_esclusioni_distr}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_PDR_list.csv ${file.path.mid1_esclusioni_pdr}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_TRATT_list.csv ${file.path.mid1_esclusioni_tratt}

log "Caricamento file esclusioni predispozione MID2" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_AM_list.csv ${file.path.mid2_esclusioni_annomese}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_DISTR_list.csv ${file.path.mid2_esclusioni_distr}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_PDR_list.csv ${file.path.mid2_esclusioni_pdr}
hdfs dfs -put -f ${PATH_ESCLUSIONI_PUBBLICAZIONE}/excl_MID_TRATT_list.csv ${file.path.mid2_esclusioni_tratt}

log "Caricamento file valori alpha" | tee -a ${LOGFILE}
hdfs dfs -put -f ${PATH_VALORI_ALPHA} ${HDFS_PATH_PATH_PROPERTIES}

log "Fine processo rilascio" | tee -a ${LOGFILE}

exit 0
