CREATE EXTERNAL TABLE IF NOT EXISTS ${hive.db}.ca_pre_final(
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
  n_coeff_correzione STRING,
  pres_tds BOOLEAN,
  tipologia_uso BOOLEAN,
  comp_termica BOOLEAN,
  cat_uso_tds STRING,
  classe_prelievo_tds STRING,
  cod_istat_last_rcu STRING,
  zona_climatica_lookup STRING,
  prelievo_annuo_prev_forced STRING,
  cod_prof_prel_std_forced STRING,
  cat_uso_forced STRING,
  zona_climatica_forced STRING,
  classe_prelievo_forced STRING,
  is_ca_calculated BOOLEAN,
  startsegment TIMESTAMP,
  endsegment TIMESTAMP,
  trattamento_forced STRING,
  massivo_freeze_executionid BIGINT,
  freeze_date TIMESTAMP
) PARTITIONED BY (anno_competenza STRING, executionid BIGINT)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION '${ca_pre_final.basepath}'
;