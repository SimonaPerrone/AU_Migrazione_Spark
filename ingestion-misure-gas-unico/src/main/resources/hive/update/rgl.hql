ALTER TABLE ${hiveconf:hive_db}.prt_cmg_rgl_p
ADD COLUMNS (
  data_comp string
  , tipo_rettifica string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , coeff_corr string
  , freq_let string
  , vol_annuo_rettificato string
  , ini_periodo string
  , fine_periodo string
  , matr_mis_giornaliere string
  , matr_conv_giornaliere string
  , annomese string
  , ammissibilita string
)
CASCADE;