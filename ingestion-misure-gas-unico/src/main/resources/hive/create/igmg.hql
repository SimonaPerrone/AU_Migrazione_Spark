CREATE EXTERNAL TABLE ${hiveconf:hive_db}.prt_cmg_igmg_p (
  t_name_file string
  , n_id_file string
  , annomese_riferimento string
  , d_caricamento string
  , local_file string
  , cod_flusso string
  , piva_utente string
  , piva_distr string
  , cod_pdr string
  , cau_int_mis string
  , cau_int_cor string
  , data_misura string
  , matr_mis_pre_int string
  , pre_conv_pre_int string
  , matr_conv_pre_int string
  , coeff_corr_pre_int string
  , gruppo_mis_int_pre_int string
  , let_misuratore_pre_int string
  , let_correttore_pre_int string
  , tipo_let string
  , rinuncia_verifica string
  , causa_stima string
  , matr_mis_post_int string
  , classe_gruppo_mis string
  , tipo_mis string
  , telegestione string
  , pre_conv_post_int string
  , matr_conv_post_int string
  , n_cifre_conv string
  , anno_fabb_conv string
  , data_inst_conv string
  , coeff_corr_post_int string
  , press_misura string
  , acc_mis string
  , n_cifre_mis string
  , anno_fabb_mis string
  , data_inst_mis string
  , gruppo_mis_int_post_int string
  , let_misuratore_post_int string
  , let_correttore_post_int string
  , data_inservizio_sm string
  , ammissibilita string
  )
PARTITIONED BY (
  annomese string
  )
STORED AS PARQUET
