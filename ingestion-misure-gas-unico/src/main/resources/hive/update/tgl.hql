ALTER TABLE ${hiveconf:hive_db}.prt_cmg_tgl_p
ADD COLUMNS (
  data_prest string
  , codprat_sii string
  , trattamento string
  , n_cifre_mis string
  , n_cifre_conv string
  , coeff_corr string
  , freq_let string
  , acc_mis string
  , raccolta string
  , causa_manc_raccolta string
  , mod_alt_racc string
  , dir_indennizzo string
  , pros_fin_inizio string
  , pros_fin_fine string
  , vol_annuo_sost string
  , classe_gruppo_mis string
  , pre_conv string
  , gruppo_mis_int string
  , esito_val string
  , note string
  , num_tentativi string
  , data_racc string
  , data_mis_eff string
  , segn_mis_eff string
  , segn_conv_eff string
  , matr_mis_giornaliere string
  , matr_conv_giornaliere string
  , annomese string
  , ammissibilita string
)
CASCADE;