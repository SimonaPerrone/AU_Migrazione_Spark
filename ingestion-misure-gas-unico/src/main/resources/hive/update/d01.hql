ALTER TABLE ${hiveconf:hive_db}.prt_cmg_d01_0150_p
ADD COLUMNS (
  mese_comp string
  , data_prest string
  , codprat_sii string
  , trattamento string
  , matr_conv string
  , n_cifre_mis string
  , n_cifre_conv string
  , coeff_corr string
  , freq_let string
  , acc_mis string
  , raccolta string
  , esito_raccolta string
  , causa_manc_raccolta string
  , mod_alt_racc string
  , dir_indennizzo string
  , pros_fin_inizio string
  , pros_fin_fine string
  , vol_annuo_sost string
  , classe_gruppo_mis string
  , pre_conv string
  , gruppo_mis_int string
  , tipo_lettura string
  , esito_val string
  , num_tentativi string
  , data_racc string
  , let_tot_prel string
  , let_tot_conv string
  , data_mis_eff string
  , segn_mis_eff string
  , segn_conv_eff string
  , ammissibilita string
)
CASCADE;