#!/bin/bash
###
# Rilascio applicativo versione 1.0
# da lanciare dopo aver spostato i dati nella posizione di rilascio (${deploy.path.local})
# ./release.sh
###
set -e

LOCAL_PATH=${deploy.path.local}
HDFS_PATH=${deploy.path.hdfs}
PATH_HIVE_STRUCT="${LOCAL_PATH}/hive"
PATH_PROPERTIES="${LOCAL_PATH}/params.properties"

echo "Inizio processo rilascio"

echo "Creazione cartella logs"
mkdir -p ${deploy.path.logs}

echo "Impostazione permessi file"
chmod 744 ${LOCAL_PATH}/*.sh

echo "Creazione strutture Hive"
hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_richiesta_pod.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_richiesta_filtro.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_cce_esito.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_cce_esito_export.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_cce_calcolo_ca.hql

hive -u jdbc:hive2://dmphclo17.siiau.local:10000 \
       -n ${hadoop_username} \
       -f ${PATH_HIVE_STRUCT}/ddl_cce_calcolo_ca_flussi.hql

echo "Creazione cartella properties"
hdfs dfs -mkdir -p ${HDFS_PATH}

echo "Caricamento file properties"
hdfs dfs -put -f ${PATH_PROPERTIES} ${HDFS_PATH}

echo "Aggiunta permessi cartelle esito per scrivere da Sqoop"
export HADOOP_USER_NAME=root
hdfs dfs -chmod 777 /user/hive/warehouse/${sqoop.db.import}.db/${sqoop.table.cceRichiestaPod}/
hdfs dfs -chmod 777 /user/hive/warehouse/${sqoop.db.import}.db/${sqoop.table.cceRichiestaFiltro}/
export HADOOP_USER_NAME=${hadoop_username}

echo "Fine processo rilascio"

exit 0
