#!/bin/bash
source <(hdfs dfs -cat "${connection.string.oracle}" \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

TABLEGSVAGGREGATOORACLE=GSV.GSV5_CONS_AGGR
TABLEGSVAGGREGATO=GSV5_CONS_AGGR

SQOOPDB=${hive.gsv}
PATHHDFSAGGREGATO=${hive.sqoop.location}/gsv5_cons_aggr


sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${TABLEGSVAGGREGATOORACLE}" \
    --columns 'n_id_gsv5_cons_aggr, n_id_gsv5_cons_richiesta, t_anno, t_ragione_sociale_cliente, t_cf_cliente, t_piva_cliente, t_codice_pdr, t_anno_mese, t_giorni_mese, n_consumo_mese, t_piva_dd, n_execution_id, t_stato, d_data_creazione' \
    --export-dir "${PATHHDFSAGGREGATO}" \
    --input-fields-terminated-by ',' \
    --input-lines-terminated-by '\n' \
    --input-null-string 'NULL' \
    --input-null-non-string 'NULL'

if [ "$?" -ne 0 ];
then
    exit 1
fi


