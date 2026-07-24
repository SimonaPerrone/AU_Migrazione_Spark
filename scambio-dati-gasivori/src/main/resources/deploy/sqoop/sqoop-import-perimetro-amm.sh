file_properties_path=${sqoop.config.path}

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop import \
    -m 1 \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "GASIVORI.GASIVORI_PERIMETRO_AMM" \
    --columns "N_ID_GASIVORI_FILE, T_NOME_FILE_IN, T_NOME_FILE_OUT, PIVA_CLIENTE, CF_CLIENTE, PRESTAZIONE, CLASSE_AGEVOLAZIONE, DATA_INIZIO, VERIFICA_AMM, COD_CAUSALE, MOTIVAZIONE" \
    --fields-terminated-by ';' \
    --lines-terminated-by '\n' \
    --null-string '\\N' \
    --null-non-string '\\N' \
    --hive-import \
    --hive-overwrite \
    --delete-target-dir \
    --hive-database "${gasivori.db}" \
    --hive-table "gasivori_perimetro_amm_c" \
    --num-mappers 10 \
    --map-column-hive N_ID_GASIVORI_FILE=bigint,T_NOME_FILE_IN=string,T_NOME_FILE_OUT=string,PIVA_CLIENTE=string,CF_CLIENTE=string,PRESTAZIONE=string,CLASSE_AGEVOLAZIONE=string,DATA_INIZIO=string,VERIFICA_AMM=int,COD_CAUSALE=string,MOTIVAZIONE=string