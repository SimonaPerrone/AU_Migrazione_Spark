CREATE EXTERNAL TABLE `cmg_gas.prt_cmg_A40_0150_p`(
	  `n_id` string,
	  `cod_servizio` string,
	  `cod_flusso` string,
	  `piva_utente` string,
	  `piva_distr` string,
	  `cod_prat_distr` string,
	  `cod_prat_utente` string,
	  `esito` string,
	  `anno_fabb_mis` string,
	  `matr_conv` string,
	  `data_attivazione` string,
	  `segn_mis` string,
	  `segn_conv` string,
	  `matr_mis` string,
	  `cod_pdr` string,
	  `note` string,
	  `anno` string,
	  `mese` string,
	  `local_file` string,
	  `t_name_file` string,
	  `n_id_file` string,
	  `d_caricamento` string)
STORED AS PARQUET 
LOCATION
  '/user/silvia/au/misure_gas_au/cmg_gas/prt_cmg_A40_0150_p'
