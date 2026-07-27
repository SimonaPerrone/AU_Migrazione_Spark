DROP TABLE IF EXISTS ca_final;

CREATE TABLE ca_final (
 id_sag_ann bigint,
 anno_competenza text,
  n_id_distr bigint,
  n_id_az_udd bigint,
  n_id_udb bigint,
  codice_remi text,
  codice_pdr text,
  pres_tds text,
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
  massivo_freeze_executionid bigint,
  executionid bigint
);

CREATE TABLE ca_final_staging (like ca_final including all);