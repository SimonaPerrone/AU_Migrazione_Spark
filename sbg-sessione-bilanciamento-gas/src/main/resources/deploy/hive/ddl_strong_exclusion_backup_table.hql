CREATE EXTERNAL TABLE ${agg.db}.strong_exclusion_backuptable_sbg
(
    pdr STRING,
    file STRING,
    cod_remi STRING,
    anno_mese STRING,
    process_type STRING
) PARTITIONED BY (execution_id BIGINT)
STORED AS PARQUET
LOCATION '${strong.exclusion.backuptable.basepath}'