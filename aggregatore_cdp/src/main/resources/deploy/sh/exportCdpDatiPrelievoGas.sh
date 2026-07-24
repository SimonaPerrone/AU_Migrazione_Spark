

export HADOOP_USER_NAME=${hadoop_username}

file_properties_path="${sqoop.config.path}"
param_properties_path="${params.config.path}"

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

TYPE=$(hdfs dfs -cat $param_properties_path | awk '/output.file.couples/ && /^[^#;]/ { print; }' |  sed 's/output.file.couples=//')

if [[ ${TYPE} != *"PRE"* && ${TYPE} != *"pre"* && (${TYPE} = *"CDP2"* || ${TYPE} = *"cdp2"*)]]; then
  sqoop export \
      --connect "${JDBC_URL}" \
      --username "${JDBC_USERNAME}" \
      --password "${JDBC_PASSWORD}" \
      --table 'CDP.CDP_DATI_PRELIEVO_GAS' \
      --columns 'anno_competenza,d_data_competenza,n_id_distr,n_id_az_udd,n_id_udb,codice_remi,codice_pdr,cat_uso,classe_prelievo,zona_climatica,id_reg_clim,cod_prof_prel_std,prelievo_annuo_prev,trattamento,tipo_trasmissione,pres_tds,d_data_rif,executionid' \
      --hcatalog-database '${hive.db}' \
      --hcatalog-table 'cdp_dati_prelievo_gas_export' \
      --input-null-string '${sqoop.null.character}' \
      --input-null-non-string '${sqoop.null.character}' \
      --staging-table 'CDP.CDP_DATI_PRELIEVO_GAS_STG' \
      --clear-staging-table \
      --num-mappers 20
fi