ALTER TABLE ${hiveconf:hive_db}.prt_cmg_sw1_p
ADD COLUMNS (
  mese_comp string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , freq_let string
  , acc_mis string
  , raccolta string
  , esito_raccolta string
  , causa_manc_raccolta string
  , mod_alt_racc string
  , dir_indennizzo string
  , pros_fin_inizio string
  , pros_fin_fine string
  , esito_val string
  , num_tentativi string
  , data_racc string
  , let_tot_prel string
  , let_tot_conv string
  , ammissibilita string
)
CASCADE;