create external table ${output.db}.rcugas_tech_ca_p_freeze
( t_codice_pdr string,
  n_id_pdr string,
  t_matricola_misuratore string,
  t_matricola_convertitore string,
  t_misuratore_integrato string,
  t_pre_conv string,
  n_coeff_correzione string,
  n_num_cifre_misuratore string,
  n_num_cifre_convertitore string,
  data_inizio_tech timestamp,
  data_fine_tech timestamp,
  freeze_date timestamp
) partitioned by (session string, execution_id bigint)
stored as parquet
location '${output.rootpath}/rcugas_tech_ca_p_freeze'