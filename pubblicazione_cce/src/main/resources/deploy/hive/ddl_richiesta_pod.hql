DROP TABLE IF EXISTS ${hive.table.cceRichiestaPod};
CREATE TABLE ${hive.table.cceRichiestaPod} (
    n_id_richiesta STRING,
    t_servizio STRING,
    t_processo STRING,
    d_data_richiesta STRING,
    t_anno STRING,
    t_mese STRING,
    t_ruolo STRING,
    t_piva STRING,
    t_codice_pod STRING,
    b_ammissibilita STRING,
    t_cod_causale STRING,
    t_motivazione STRING,
    t_nome_file STRING,
    t_tipo_amm STRING,
    sqoop_date STRING
)
PARTITIONED BY (partition_request_date STRING)
STORED AS PARQUET;
