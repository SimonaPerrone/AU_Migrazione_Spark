export REFRESH_SBG_MISURE=$(cat <<-EOF
CREATE EXTERNAL TABLE IF NOT EXISTS ${py.hive.table.sbgmisure.db}.${py.hive.table.sbgmisure.name} (
    cod_pdr STRING,
    --piva_distr STRING,
    piva_it STRING,
    piva_udd STRING,
    piva_udb STRING,
    piva_rdb STRING,
    cod_remi STRING,
    id_reg_clim STRING,
    cod_prof_std STRING,
    trattamento STRING,
    trattamento_calcolo STRING,
    giorno STRING,
    consumo DOUBLE,
    tipo_cliente STRING,
    unit_mis_prel STRING,
    data_insert STRING,
    sessione_sbg STRING
)
PARTITIONED BY (annomese_rif STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/${py.hive.table.sbgmisure.db}.db/${py.hive.table.sbgmisure.name}'
;

MSCK REPAIR TABLE ${py.hive.table.sbgmisure.db}.${py.hive.table.sbgmisure.name};

--ANALYZE TABLE  ${py.hive.table.sbgmisure.db}.${py.hive.table.sbgmisure.name} PARTITION(annomese_rif) COMPUTE STATISTICS;
EOF
)