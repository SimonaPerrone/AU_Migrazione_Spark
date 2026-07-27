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
    where cast(date_format(D_DATA_MIS_EFF_TS,'yyyyMM') as int)  >= cast(date_format(date_sub(current_date(),396),'yyyyMM') as int)
    and t_tipo_lettura='E';

--select
--T_CODICE_PDR as cod_pdr,
--CONCAT(YEAR(from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))),LPAD(MONTH(from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))),2,0)) as annomese_riferimento, 
--            from_unixtime(unix_timestamp(D_DATA_MIS_EFF , 'yyyy-MM-dd HH:mm:ss'))  as dt_caricamento,
--            case when t_segn_mis_eff is null then t_segn_mis_sost
--            else     t_segn_mis_eff    End    as let_tot_prel,
--            from_unixtime(unix_timestamp(D_DATA_MIS_EFF , 'yyyy-MM-dd HH:mm:ss')) as data_racc,
--            "E" as tipo_lettura
--             FROM SWITCH_GAS.PRT_VTG6_p            
--    where from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd')) 
--    and t_tipo_lettura='E';

