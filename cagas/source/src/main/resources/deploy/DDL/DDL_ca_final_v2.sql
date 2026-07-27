CREATE EXTERNAL TABLE IF NOT EXISTS ${hive.db}.ca_final_v2(
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
  codistat STRING,
  pres_tds BOOLEAN,
  massivo_freeze_executionid BIGINT,
  freeze_date TIMESTAMP,
  piva_distr STRING,
  piva_udd STRING,
  piva_udb STRING,
) PARTITIONED BY (session STRING, tipo_trasmissione STRING, anno_competenza STRING, executionid BIGINT)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
  LOCATION '${ca_final.basepath}'
;