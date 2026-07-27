--da disattivare momentaneamente 
--e riattivare in seguito tramite union con la TMV

drop table SWITCH_GAS.PRT_VTG6_o;
Create Table SWITCH_GAS.PRT_VTG6_o Stored as Parquet
as
select T_CODICE_PDR as cod_pdr,
CONCAT(YEAR(D_DATA_MIS_EFF_TS),LPAD(MONTH(D_DATA_MIS_EFF_TS),2,0)) as annomese_riferimento, 
            D_DATA_MIS_EFF_TS  as dt_caricamento,
            cast(case when NVL(t_segn_mis_eff,'') == '' then t_segn_mis_sost
            else     t_segn_mis_eff    End as double)   as let_tot_prel,
            D_DATA_MIS_EFF_TS as data_racc,
            "E" as tipo_lettura
    FROM SWITCH_GAS.PRT_VTG6_p            
    where CAST(date_format(d_caricamento,'yyyyMMdd') as int) >=  ${hiveconf:last_dt_elab_vtg6} and
	cast(date_format(D_DATA_MIS_EFF_TS,'yyyyMM') as int)  >= cast(date_format(date_sub(current_date(),${env:limit_gg_gas}),'yyyyMM') as int)
    and t_tipo_lettura='E';
	




