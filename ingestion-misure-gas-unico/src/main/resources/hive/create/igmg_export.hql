CREATE EXTERNAL TABLE ${hiveconf:hive_db}.prt_cmg_igmg_export (
  cod_pdr string
  , cau_int_mis string
  , cau_int_cor string
  , matr_mis_post_int string
  , tipo_mis string
  , telegestione string
  , coeff_corr_post_int string
  , classe_gruppo_mis string
  , acc_mis string
  , n_cifre_mis string
  , anno_fabb_mis string
  , data_inst_mis string
  , gruppo_mis_int_post_int string
  , pre_conv_post_int string
  , matr_conv_post_int string
  , n_cifre_conv string
  , anno_fabb_conv string
  , data_inst_conv string
  , press_misura string
  , data_misura string
  , piva_utente string
  , piva_distr string
  , t_name_file string
  , ammissibilita string
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
LOCATION '/user/hive/warehouse/au.db/misure_gas_au/cmg_gas/prt_cmg_igmg_export';
