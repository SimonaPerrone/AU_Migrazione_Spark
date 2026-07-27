hdfs dfs -mkdir /user/development/test/
hdfs dfs -mkdir /user/development/test/ca_final
hdfs dfs -put ca_final/* /user/development/test/ca_final

DROP TABLE IF EXISTS sqoop_test.ca_final;

CREATE EXTERNAL TABLE sqoop_test.ca_final(
  id_sag_ann BIGINT,
  n_id_distr BIGINT,
  n_id_az_udd BIGINT,
  n_id_udb BIGINT,
  codice_remi STRING,
  codice_pdr STRING,
  cap_trasp_pdr STRING,
  cat_uso STRING,
  classe_prelievo STRING,
  zona_climatica STRING,
  id_reg_clim STRING,
  cod_prof_prel_std STRING,
  prelievo_annuo_prev STRING,
  trattamento STRING,
  d_ricezione STRING,
  tipo_trasmissione STRING,
  codistat STRING,
  id_ca_error_code INT,
  start_local_file STRING,
  end_local_file STRING,
  calcmode STRING,
  start_t_misuratore_integrato STRING,
  end_t_misuratore_integrato STRING,
  start_t_pre_conv STRING,
  end_t_pre_conv STRING,
  pres_tds BOOLEAN,
  tipologia_uso BOOLEAN,
  comp_termica BOOLEAN,
  cat_uso_tds STRING,
  classe_prelievo_tds STRING,
  cod_istat_last_rcu STRING,
  zona_climatica_lookup STRING,
  prelievo_annuo_prev_forced STRING,
  cod_prof_prel_std_forced STRING,
  is_ca_calculated BOOLEAN
) PARTITIONED BY (anno_competenza STRING,executionid STRING)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION '/user/development/test/ca_final'
;

CREATE TABLE ca_final_test (
anno_competenza text,
executionid bigint,
 id_sag_ann bigint,
  n_id_distr bigint,
  n_id_az_udd bigint,
  n_id_udb bigint,
  codice_remi text,
  codice_pdr text,
  cap_trasp_pdr text,
  cat_uso text,
  classe_prelievo text,
  zona_climatica text,
  id_reg_clim text,
  cod_prof_prel_std text,
  prelievo_annuo_prev text,
  trattamento text,
  d_ricezione text,
  tipo_trasmissione text,
  codistat text,
  id_ca_error_code integer,
  start_local_file text,
  end_local_file text,
  calcmode text,
  start_t_misuratore_integrato text,
  end_t_misuratore_integrato text,
  start_t_pre_conv text,
  end_t_pre_conv text,
  pres_tds boolean,
  tipologia_uso boolean,
  comp_termica boolean,
  cat_uso_tds text,
  classe_prelievo_tds text,
  cod_istat_last_rcu text,
  zona_climatica_lookup text,
  prelievo_annuo_prev_forced text,
  cod_prof_prel_std_forced text,
  is_ca_calculated boolean
);

CREATE TABLE ca_final_test_staging (like ca_final_test including all);
# after create table, to "view" partition
msck repair table sqoop_test.ca_final;

script ingestion sqoop:

#!/usr/bin/env bash

file_properties_path="/apps/deploy/sqoop.properties"

JDBC_USERNAME=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.user' |  sed 's/spark.app.user=//')
JDBC_URL=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.url' |  sed 's/spark.app.url=//')
JDBC_PASSWORD=$(hdfs dfs -cat $file_properties_path | grep 'spark.app.password' |  sed 's/spark.app.password=//')

sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table 'ca_final_test' \
    --hcatalog-database 'sqoop_test' \
    --hcatalog-table 'ca_final' \
    --staging-table 'ca_final_test_staging' \
    --hcatalog-partition-keys 'anno_competenza,executionid' \
    --hcatalog-partition-values '2021,1612526114702' \
    --clear-staging-table \
    --num-mappers 1


