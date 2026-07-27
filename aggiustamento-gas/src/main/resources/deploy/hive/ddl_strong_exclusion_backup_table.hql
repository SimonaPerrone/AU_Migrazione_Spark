CREATE EXTERNAL TABLE ${agg.db}.strong_exclusion_backuptable_agg
(
    pdr STRING,
    file STRING,
    cod_remi STRING,
    anno_mese STRING,
    exclusion_type STRING,
    process_type STRING
) PARTITIONED BY (execution_id BIGINT)
STORED AS PARQUET
LOCATION '${strong.exclusion.backuptable.basepath}'