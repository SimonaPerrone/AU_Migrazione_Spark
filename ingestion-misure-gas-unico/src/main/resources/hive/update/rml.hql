ALTER TABLE ${hiveconf:hive_db}.prt_cmg_rml_p
ADD COLUMNS (
  mese_comp string
  , tipo_rettifica string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , vol_annuo_rettificato string
  , periodo_ric string
  , ammissibilita string
)
CASCADE;