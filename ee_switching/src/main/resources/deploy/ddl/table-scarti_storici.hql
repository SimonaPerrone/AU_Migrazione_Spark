CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.scarti_storici
(
pod14 string,
d_data_decorrenza string,
d_caricamento timestamp,
nome_flusso string,
t_cod_contr_disp string
)
PARTITIONED BY
(
annomese_sw string
)
STORED AS PARQUET;
