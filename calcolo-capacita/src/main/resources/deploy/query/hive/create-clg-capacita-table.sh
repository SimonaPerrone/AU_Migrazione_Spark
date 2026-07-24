export CREATE_CLG_CAPACITA_TABLE=$(cat <<-EOF
DROP TABLE IF EXISTS ${hive.table.result.db}.${hive.table.result.name};
CREATE TABLE ${hive.table.result.db}.${hive.table.result.name}(

n_id_pdr_capacita_tmp string, --valorizzato da altri applicativi
n_id_pdr string,
n_id_pratica string, --valorizzato da altri applicativi
t_codice_pdr string,
d_data_rif string,
d_data_da string,
d_data_a string,
d_data_inizio string,
n_anno INT,
n_mese INT,
t_tipo_calcolo string,
n_pcm decimal(30,10),
n_ctc decimal(30,10),
t_origine string,
t_processo_origine string,
t_esito_calcolo string,
t_esito_code_desc string,
d_data_inserimento string,
n_execution_id string,
t_esito_agg_rcu string, --valorizzato da altri applicativi
t_errore_agg_rcu string, --valorizzato da altri applicativi
t_esito_agg_rcu_desc string, --valorizzato da altri applicativi
t_stato  string, --valorizzato da altri applicativi
d_data_aggiornamento  string --valorizzato da altri applicativi

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS ${hive.table.result.db}.${hive.table.result.name}_history(

n_id_pdr_capacita_tmp string, --valorizzato da altri applicativi
n_id_pdr string,
n_id_pratica string, --valorizzato da altri applicativi
t_codice_pdr string,
d_data_rif string,
d_data_da string,
d_data_a string,
d_data_inizio string,
n_anno INT,
n_mese INT,
t_tipo_calcolo string,
n_pcm decimal(30,10),
n_ctc decimal(30,10),
t_origine string,
t_processo_origine string,
t_esito_calcolo string,
t_esito_code_desc string,
d_data_inserimento string,
t_esito_agg_rcu string, --valorizzato da altri applicativi
t_errore_agg_rcu string, --valorizzato da altri applicativi
t_esito_agg_rcu_desc string, --valorizzato da altri applicativi
t_stato  string, --valorizzato da altri applicativi
d_data_aggiornamento  string --valorizzato da altri applicativi
)
PARTITIONED BY (n_execution_id STRING)
STORED AS PARQUET
EOF
)
