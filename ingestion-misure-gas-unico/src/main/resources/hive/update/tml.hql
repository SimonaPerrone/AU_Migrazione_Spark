ALTER TABLE ${hiveconf:hive_db}.prt_cmg_tml_p
ADD COLUMNS (
  mese_comp string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , n_cifre_mis string
  , n_cifre_conv string
  , raccolta string
  , pros_fin_inizio string
  , pros_fin_fine string
  , vol_annuo_sost string
  , classe_gruppo_mis string
  , pre_conv string
  , gruppo_mis_int string
  , esito_val string
  , note string
  , data_mis_eff string
  , segn_mis_eff string
  , segn_conv_eff string
  , ammissibilita string
)
CASCADE;