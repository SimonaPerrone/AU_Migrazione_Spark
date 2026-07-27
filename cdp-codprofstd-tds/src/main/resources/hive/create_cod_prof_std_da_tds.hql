create external table ${output.db}.cod_prof_std_da_tds
( codice_pdr string,
  n_id_distr string,
  n_id_udd string,
  n_id_udb string,
  cat_uso string,
  cod_remi string,
  zona_climatica string,
  classe_prelievo string,
  id_regione_climatica string,
  prelievo_annuo_prev string,
  trattamento string,
  cod_prof_prel_std string,
  cod_prof_prel_std_calc string,
  data_fine_for timestamp,
  data_creazione timestamp,
  error_log string,
  cod_prof_prel_std_forced string,
  prelievo_annuo_prev_forced string,
  cat_uso_forced string,
  zona_climatica_forced string,
  classe_prelievo_forced string,
  trattamento_forced string,
  pres_tds string,
  massivo_freeze_execution_id bigint,
  massivo_freeze_date timestamp
) partitioned by (anno_competenza int, execution_id bigint)
stored as parquet
location '${output.rootpath}/cod_prof_std_da_tds'