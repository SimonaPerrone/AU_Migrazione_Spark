CREATE EXTERNAL TABLE au.TAB_DATI_SETTLE_SAG_RES_ORACLE_LAST (
    id_sag_ann int, 
    anno_competenza string, 
    n_id_distr string, 
    n_id_az_udd string, 
    codice_remi string, 
    codice_pdr string, 
    cap_trasp_pdr string, 
    cat_uso string, 
    classe_prelievo string, 
    zona_climatica string, 
    id_reg_clim string, 
    cod_prof_prel_std string, 
    prelievo_annuo_prev int, 
    trattamento string, 
    d_ricezione string, 
    ranking int
) 
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_DATI_SETTLE_SAG_RES_ORACLE/${CURRENT_DATE}';
