create external table ${tsg.db}.${tsg.ReportAmmissibilitaTFC.tableName}
(
    n_id_tsg2_file bigint,
    nome_file string,
    data string,
    id_reg_clim bigint,
    wkr double,
    numero_riga string,
    tipo_file string,
    piva_utente string,
    verifica_amm boolean,
    cod_causale string,
    motivazione string,
    data_amm timestamp,
    annomese string,
    progressivo string,
    executionid bigint
) partitioned by (annomese_ricezione string)
stored as parquet
location '${tsg.ReportAmmissibilitaTFC.basepath}'