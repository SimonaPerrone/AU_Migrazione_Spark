hdfs dfs -mkdir /user/development/
hdfs dfs -mkdir /user/development/test/
hdfs dfs -mkdir /user/development/test/ca_final
hdfs dfs -put ca_final/* /user/development/test/ca_final

# after create table, to "view" partition
msck repair table sqoop_test.ca_final;

script ingestion sqoop:

#!/usr/bin/env bash
set -e

# load bash JDBC variables from Java properties file in HDFS
source <(hdfs dfs -cat '/apps/deploy/sqoop.properties' \
    | sed 's/\(.+\?\)\=\(.*\)$/\2='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )

sqoop export \
    --connect "jdbc:postgresql://au-m01:5432/test_db" \
    --username "test_user" \
    --password "test_user" \
    --table 'ca_final_test' \
    --columns 'anno_competenza,executionid,id_sag_ann,n_id_distr,n_id_az_udd,n_id_udb,codice_remi,codice_pdr,cap_trasp_pdr,cat_uso,classe_prelievo,zona_climatica,id_reg_clim,cod_prof_prel_std,prelievo_annuo_prev,trattamento,d_ricezione,tipo_trasmissione,codistat,id_ca_error_code,start_local_file,end_local_file,calcmode,start_t_misuratore_integrato,end_t_misuratore_integrato,start_t_pre_conv,end_t_pre_conv,pres_tds,tipologia_uso,comp_termica,cat_uso_tds,classe_prelievo_tds,cod_istat_last_rcu,zona_climatica_lookup,prelievo_annuo_prev_forced,cod_prof_prel_std_forced,is_ca_calculated' \
    --export-dir '/user/development/test/ca_final_pre' \
    --input-fields-terminated-by ',' \
    --input-lines-terminated-by '\n' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --staging-table 'ca_final_test_staging' \
    --clear-staging-table \
    --num-mappers 1

    --input-lines-terminated-by '\n' \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --export-dir '/user/development/test/ca_final/anno_competenza=2021/executionid=1612526114702/' \


DROP TABLE IF EXISTS sqoop_test.ca_final_pre;
CREATE EXTERNAL TABLE sqoop_test.ca_final_pre(
anno_competenza STRING,
executionid BIGINT,
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
  is_ca_calculated BOOLEAN)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'=',',
  'serialization.format'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/development/test/ca_final_pre';

insert overwrite table sqoop_test.ca_final_pre
 select anno_competenza ,
executionid ,
  id_sag_ann ,
  n_id_distr ,
  n_id_az_udd ,
  n_id_udb ,
  codice_remi ,
  codice_pdr ,
  cap_trasp_pdr ,
  cat_uso ,
  classe_prelievo ,
  zona_climatica ,
  id_reg_clim ,
  cod_prof_prel_std ,
  prelievo_annuo_prev ,
  trattamento ,
  d_ricezione ,
  tipo_trasmissione ,
  codistat ,
  id_ca_error_code ,
  start_local_file ,
  end_local_file ,
  calcmode ,
  start_t_misuratore_integrato ,
  end_t_misuratore_integrato,
  start_t_pre_conv ,
  end_t_pre_conv ,
  pres_tds ,
  tipologia_uso ,
  comp_termica ,
  cat_uso_tds ,
  classe_prelievo_tds ,
  cod_istat_last_rcu ,
  zona_climatica_lookup ,
  prelievo_annuo_prev_forced ,
  cod_prof_prel_std_forced ,
  is_ca_calculated  from sqoop_test.ca_final where anno_competenza='2021' and executionid='1612526114702';

  select * from ca_final_pre;
 insert overwrite table sqoop_test.ca_final_pre
 select anno_competenza,executionid,* from sqoop_test.ca_final where anno_competenza='2021' and executionid='1612526114702';