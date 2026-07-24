create external table ${gse.db}.gse_aggr_m_export
(  n_id_gse_richiesta_er_m bigint,
     t_mese_anno string,
     t_cod_pod string,
     n_consumo_mensile decimal(12,3),
     d_data_creazione timestamp,
     n_execution_id bigint
)
stored as parquet
location '${hdfs.output.path}/gse_aggr_m_export'