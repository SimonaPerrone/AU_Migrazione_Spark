CREATE TABLE IF NOT EXISTS ${hiveconf:switching_db}.report_file
(
piva_distributore string
, piva_utente string
, pod string
, nome_flusso string
, percorso_file string
, validation_output string
, d_creazione timestamp
)
PARTITIONED BY
(
annomese_sw string
)
STORED AS PARQUET;
