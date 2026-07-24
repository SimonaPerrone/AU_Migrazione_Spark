ALTER TABLE ${hiveconf:hive_db}.prt_cmg_rsl_p
ADD COLUMNS (
  mese_comp string
  , tipo_rettifica string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , freq_let string
  , vol_annuo_rettificato string
  , data_racc string
  , vol_ric string
  , ini_periodo string
  , fine_periodo string
  , periodo_ric string
  , ammissibilita string
)
CASCADE;