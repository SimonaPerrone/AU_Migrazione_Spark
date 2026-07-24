create external table ${cdp.db}.cdp_dati_prelievo_gas
(  n_id_az_udd string,
   piva_udd string,
   n_id_distr string,
   piva_distr string,
   n_id_udb string,
   piva_udb string,
   codice_pdr string,
   codice_remi string,
   cat_uso string,
   classe_prelievo string,
   zona_climatica string,
   id_reg_clim string,
   cod_prof_prel_std string,
   prelievo_annuo_prev string,
   trattamento string,
   pres_tds string,
   d_data_competenza timestamp,
   udd_oggetto_swithcing string,
   tipo_trasmissione string,
   calc_executiond_id string,
   massivo_freezer_executiond_id string,
   d_data_rif timestamp
) partitioned by (anno_competenza string, executionid bigint)
stored as parquet
location '${hdfs.output.basepath.csv}'