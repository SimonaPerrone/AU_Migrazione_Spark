#!/bin/bash
source <(hdfs dfs -cat "${connection.string.oracle}" \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

source sqoopconfigs.conf

TABLESGSPERIMETROSWGSRAWORACLE=SGS_PERIMETRO_SWG_S_RAW
TABLESGSPERIMETROSWGSRAW=SGS_PERIMETRO_SWG_S_RAW_C

TABLESGSPERIMETROSWGARAWORACLE=SGS_PERIMETRO_SWG_A_RAW
TABLESGSPERIMETROSWGARAW=SGS_PERIMETRO_SWG_A_RAW_C

TABLESGSPERIMETROUIGSRAWORACLE=SGS_PERIMETRO_UIG_S_RAW
TABLESGSPERIMETROUIGSRAW=SGS_PERIMETRO_UIG_S_RAW_C

TABLESGSPERIMETROUIGARAWORACLE=SGS_PERIMETRO_UIG_A_RAW
TABLESGSPERIMETROUIGARAW=SGS_PERIMETRO_UIG_A_RAW_C

TABLESGSPERIMETROVTGSRAWORACLE=SGS_PERIMETRO_VTG_S_RAW
TABLESGSPERIMETROVTGSRAW=SGS_PERIMETRO_VTG_S_RAW_C

SQOOPDB=${hive.sgs.db}
PATHHDFSSGSPERIMETROSWGSRAWC=${hive.sgsPerimetroSwgSRawCPath}
PATHHDFSSGSPERIMETROSWGARAWC=${hive.sgsPerimetroSwgARawCPath}
PATHHDFSSGSPERIMETROUIGSRAWC=${hive.sgsPerimetroUigSRawCPath}
PATHHDFSSGSPERIMETROUIGARAWC=${hive.sgsPerimetroUigARawCPath}
PATHHDFSSGSPERIMETROVTGSRAWC=${hive.sgsPerimetroVtgSRawCPath}

if [ "$BOOL_PERIMETRO_SWG_S" == "true" ]; then

  echo "Import SGS_PERIMETRO_SWG_S_RAW_C"

  hdfs dfs -rm  ${PATHHDFSSGSPERIMETROSWGSRAWC}/*

  sqoop import \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table "${TABLESGSPERIMETROSWGSRAWORACLE}" \
      --hive-import \
      --hive-database "${SQOOPDB}" \
      --hive-table "${TABLESGSPERIMETROSWGSRAW}" \
      --create-hive-table \
      --fields-terminated-by ';' \
      --lines-terminated-by '\n' \
      --null-string '\\N' \
      --null-non-string '\\N' \
      --split-by "N_ID_PRATICA" \
      --num-mappers 2 \
      --delete-target-dir \
      --target-dir "${PATHHDFSSGSPERIMETROSWGSRAWC}" \
      --map-column-hive N_ID_PRATICA=string,T_STATO_PRATICA=string,B_AMMISSIBILE=string,T_STATO=string,D_DATA_DECORRENZA=string,T_CODICE_PDR=string,N_ID_PDR=string,PIVA_UDD_ENTRANTE=string,PIVA_UDB_ENTRANTE=string,PIVA_UDB_USCENTE=string,DATA_ESTRAZIONE=string

fi


if [ "$BOOL_PERIMETRO_SWG_A" == "true" ]; then

  echo "Import SGS_PERIMETRO_SWG_A_RAW_C"

  hdfs dfs -rm  ${PATHHDFSSGSPERIMETROSWGARAWC}/*

  sqoop import \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table "${TABLESGSPERIMETROSWGARAWORACLE}" \
      --hive-import \
      --hive-database "${SQOOPDB}" \
      --hive-table "${TABLESGSPERIMETROSWGARAW}" \
      --create-hive-table \
      --fields-terminated-by ';' \
      --lines-terminated-by '\n' \
      --null-string '\\N' \
      --null-non-string '\\N' \
      --split-by "N_ID_PRATICA" \
      --num-mappers 2 \
      --delete-target-dir \
      --target-dir "${PATHHDFSSGSPERIMETROSWGARAWC}" \
      --map-column-hive N_ID_PRATICA=string,D_DATA_DECORRENZA=string,T_CODICE_PDR=string,N_ID_PDR=string,PIVA_UDB_ENTRANTE=string,DATA_ESTRAZIONE=string

fi

if [ "$BOOL_PERIMETRO_UIG_S" == "true" ]; then

  echo "Import SGS_PERIMETRO_UIG_S_RAW_C"

  hdfs dfs -rm  ${PATHHDFSSGSPERIMETROUIGSRAWC}/*

  sqoop import \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table "${TABLESGSPERIMETROUIGSRAWORACLE}" \
      --hive-import \
      --hive-database "${SQOOPDB}" \
      --hive-table "${TABLESGSPERIMETROUIGSRAW}" \
      --create-hive-table \
      --fields-terminated-by ';' \
      --lines-terminated-by '\n' \
      --null-string '\\N' \
      --null-non-string '\\N' \
      --split-by "N_ID_PRATICA" \
      --num-mappers 2 \
      --delete-target-dir \
      --target-dir "${PATHHDFSSGSPERIMETROUIGSRAWC}" \
      --map-column-hive N_ID_PRATICA=string,T_STATO_PRATICA=string,B_AMMISSIBILE=string,T_STATO=string,D_DATA_DECORRENZA=string,T_CODICE_PDR=string,N_ID_PDR=string,PIVA_UDD_ENTRANTE=string,PIVA_UDB_ENTRANTE=string,PIVA_UDB_USCENTE=string,DATA_ESTRAZIONE=string

fi

if [ "$BOOL_PERIMETRO_UIG_A" == "true" ]; then

  echo "Import SGS_PERIMETRO_UIG_A_RAW_C"


  hdfs dfs -rm  ${PATHHDFSSGSPERIMETROUIGARAWC}/*

  sqoop import \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table "${TABLESGSPERIMETROUIGARAWORACLE}" \
      --hive-import \
      --hive-database "${SQOOPDB}" \
      --hive-table "${TABLESGSPERIMETROUIGARAW}" \
      --create-hive-table \
      --fields-terminated-by ';' \
      --lines-terminated-by '\n' \
      --null-string '\\N' \
      --null-non-string '\\N' \
      --split-by "N_ID_PRATICA" \
      --num-mappers 2 \
      --delete-target-dir \
      --target-dir "${PATHHDFSSGSPERIMETROUIGARAWC}" \
      --map-column-hive N_ID_PRATICA=string,D_DATA_DECORRENZA=string,T_CODICE_PDR=string,N_ID_PDR=string,PIVA_UDB_ENTRANTE=string,DATA_ESTRAZIONE=string

fi

if [ "$BOOL_PERIMETRO_VTG_S" == "true" ]; then

  echo "Import SGS_PERIMETRO_VTG_S_RAW_C"

  hdfs dfs -rm  ${PATHHDFSSGSPERIMETROVTGSRAWC}/*

  sqoop import \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table "${TABLESGSPERIMETROVTGSRAWORACLE}" \
      --hive-import \
      --hive-database "${SQOOPDB}" \
      --hive-table "${TABLESGSPERIMETROVTGSRAW}" \
      --create-hive-table \
      --fields-terminated-by ';' \
      --lines-terminated-by '\n' \
      --null-string '\\N' \
      --null-non-string '\\N' \
      --split-by "N_ID_PRATICA" \
      --num-mappers 2 \
      --delete-target-dir \
      --target-dir "${PATHHDFSSGSPERIMETROVTGSRAWC}" \
      --map-column-hive N_ID_PRATICA=string,T_STATO_PRATICA=string,B_AMMISSIBILE=string,T_STATO=string,D_DATA_DECORRENZA=string,T_CODICE_PDR=string,N_ID_PDR=string,PIVA_UDD_ENTRANTE=string,PIVA_UDB_ENTRANTE=string,PIVA_UDB_USCENTE=string,DATA_ESTRAZIONE=string

fi

if [ "$?" -ne 0 ];
then
    exit 1
fi