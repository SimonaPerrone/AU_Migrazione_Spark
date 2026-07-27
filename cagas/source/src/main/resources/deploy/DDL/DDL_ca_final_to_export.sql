DROP TABLE IF EXISTS ${hive.db}.ca_final_to_export;

CREATE EXTERNAL TABLE ${hive.db}.ca_final_to_export(
  id_sag_ann BIGINT,
  anno_competenza STRING,
  n_id_distr BIGINT,
  n_id_az_udd BIGINT,
  n_id_udb BIGINT,
  codice_remi STRING,
  codice_pdr STRING,
  pres_tds STRING,
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
  massivo_freeze_executionid BIGINT,
   executionid BIGINT
)
  ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
  STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
  OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION '${ca_final_to_export.basepath}';
