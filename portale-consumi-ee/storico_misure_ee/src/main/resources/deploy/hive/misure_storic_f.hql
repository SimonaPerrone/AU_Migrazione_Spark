DROP TABLE IF EXISTS ${hive.table.misure.misure_storic_f};
CREATE TABLE ${hive.table.misure.misure_storic_f} (
    cf_piva STRING,
    pod STRING,
    data_lettura STRING,
    data_ricezione STRING,
    motivazione STRING,
    lettura_monoraria DOUBLE,
    lettura_f1 DOUBLE,
    lettura_f2 DOUBLE,
    lettura_f3 DOUBLE,
    lettura_f4 DOUBLE,
    lettura_f5 DOUBLE,
    lettura_f6 DOUBLE,
    ea STRING,
    er STRING,
    tipo_flusso STRING,
    data_lettura_num BIGINT
)
PARTITIONED BY (
    annomese_riferimento INT,
    cod_pod CHAR(2),
    is_mis_oraria CHAR(1)
);
