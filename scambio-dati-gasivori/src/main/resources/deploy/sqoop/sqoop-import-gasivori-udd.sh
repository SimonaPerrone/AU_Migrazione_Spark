file_properties_path=${sqoop.config.path}

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "GASIVORI.GASIVORI_FILIERA_UDD" \
    --columns "N_ID_GASIVORI_FILIERA_UDD, T_RAGIONE_SOCIALE_CLIENTE, T_PIVA_CLIENTE, T_CF_CLIENTE, T_CODICE_PDR, T_CLASSE_AGEVOLAZIONE, T_ANNO_RIF, D_DATA_INIZIO, D_DATA_FINE, UDD_DEST, T_EXECUTIONID, D_DATA_INSERIMENTO" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-import \
    --hive-overwrite \
    --delete-target-dir \
    --hive-database "${gasivori.db}" \
    --hive-table "gasivori_filiera_udd_c" \
    --split-by "N_ID_GASIVORI_FILIERA_UDD" \
    --num-mappers 10 \
    --map-column-hive N_ID_GASIVORI_FILIERA_UDD=bigint,T_RAGIONE_SOCIALE_CLIENTE=string,T_PIVA_CLIENTE=string,T_CF_CLIENTE=string,T_CODICE_PDR=string,T_CLASSE_AGEVOLAZIONE=string,T_ANNO_RIF=string,D_DATA_INIZIO=timestamp,D_DATA_FINE=timestamp,UDD_DEST=string,T_EXECUTIONID=bigint,D_DATA_INSERIMENTO=timestamp