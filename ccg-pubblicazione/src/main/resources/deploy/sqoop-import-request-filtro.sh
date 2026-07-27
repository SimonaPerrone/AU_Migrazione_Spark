export HADOOP_USER_NAME=${hadoop.username}

file_properties_path=${sqoop.config.path}

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

while getopts ":e:" opt; do
  case $opt in
    e) DATA_RICHIESTA="$OPTARG"
    ;;
    \?)
    echo "Invalid option -$OPTARG" >&2
    ;;
  esac
done

SQOOP_DATE=$(date +'%Y-%m-%d %H:%M:%S')
echo "sqoop date: $SQOOP_DATE"
echo "sqoop data richiesta: $DATA_RICHIESTA"

sqoop import \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --query "select N_ID_RICHIESTA,T_SERVIZIO,T_PROCESSO,D_DATA_RICHIESTA,T_ANNO,T_MESE,T_RUOLO,T_PIVA,T_COD_REMI,T_INCOERENTI,T_TRATTAMENTO,T_PIVA_UDD,T_PIVA_UDB,T_PIVA_ID,T_CODPROFSTD, '${SQOOP_DATE}' AS SQOOP_DATE from CCG.CCG_RICHIESTA_FILTRO where D_DATA_RICHIESTA >= to_date('${DATA_RICHIESTA}', 'YYYY-MM-DD') AND D_DATA_RICHIESTA - 1 < to_date('${DATA_RICHIESTA}', 'YYYY-MM-DD') AND \$CONDITIONS" \
    --target-dir "${table.path.hdfs}/CCG_RICHIESTA_FILTRO" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-import \
    --hive-database "${ccg.db}" \
    --hive-table "CCG_RICHIESTA_FILTRO" \
    --split-by "N_ID_RICHIESTA" \
    --num-mappers 10 \
    --delete-target-dir \
    --hive-partition-key 'PARTITION_REQUEST_DATE' \
    --hive-partition-value "${DATA_RICHIESTA}" \
    --hive-overwrite \
    --map-column-hive N_ID_RICHIESTA=string,T_SERVIZIO=string,T_PROCESSO=string,D_DATA_RICHIESTA=timestamp,T_ANNO=string,T_MESE=string,T_RUOLO=string,T_PIVA=string,T_COD_REMI=string,T_INCOERENTI=string,T_TRATTAMENTO=string,T_PIVA_UDD=string,T_PIVA_UDB=string,T_PIVA_ID=string,T_CODPROFSTD=string,SQOOP_DATE=timestamp