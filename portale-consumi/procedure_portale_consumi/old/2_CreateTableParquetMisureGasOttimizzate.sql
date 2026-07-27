drop TABLE CMG.PRT_CMG_TML_o;
Create Table CMG.PRT_CMG_TML_o  Stored As Parquet
as
select		cod_pdr,
            annomese_riferimento,
            from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
            tipo_lettura            
from
CMG.PRT_CMG_TML_p
where tipo_lettura ="E"      
and  from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd')); 


drop table CMG.PRT_CMG_RML_o; 
Create Table CMG.PRT_CMG_RML_o Stored As Parquet
as
select		cod_pdr,
            annomese_riferimento,
            from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
			mot_rett_lett as Motivazione	
from
CMG.PRT_CMG_RML_p
where  from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd')) ;

drop table SWITCH_GAS.PRT_VTG6_o;
Create Table SWITCH_GAS.PRT_VTG6_o Stored as Parquet
as
select
T_CODICE_PDR as cod_pdr,
CONCAT(YEAR(from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))),LPAD(MONTH(from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))),2,0)) as annomese_riferimento, 
            from_unixtime(unix_timestamp(D_DATA_MIS_EFF , 'yyyy-MM-dd HH:mm:ss'))  as dt_caricamento,
            case when t_segn_mis_eff is null then t_segn_mis_sost
			else 	t_segn_mis_eff	End	as let_tot_prel,
            from_unixtime(unix_timestamp(D_DATA_MIS_EFF , 'yyyy-MM-dd HH:mm:ss')) as data_racc,
            "E" as tipo_lettura
             FROM SWITCH_GAS.PRT_VTG6_p			
	where from_unixtime(unix_timestamp(D_DATA_MIS_EFF ,'yyyy-MM-dd HH:mm:ss'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd')) 
	and t_tipo_lettura='E';
	
	
drop Table 	CMG.PRT_CMG_TAL_o;
Create Table CMG.PRT_CMG_TAL_o Stored As Parquet
as 
select
    cod_pdr,
    annomese_riferimento,
    from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
    let_tot_prel,
    from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy')) as data_lettura    
from CMG.PRT_CMG_TAL_p
where from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd'));

drop table CMG.PRT_CMG_TAV_o;
Create Table CMG.PRT_CMG_TAV_o Stored As Parquet
as 
select
	cod_pdr,
    annomese_riferimento,
    from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
    let_tot_prel,
    from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy')) as data_lettura    
from CMG.PRT_CMG_TAV_p
where from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd'));

drop table CMG.PRT_CMG_TGL_o;
create table CMG.PRT_CMG_TGL_o Stored As Parquet as 
    select  cod_pdr,  
            mese_comp, 
            from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
            tipo_lettura            
    from CMG.PRT_CMG_TGL_p
	where tipo_lettura ="E"
	and  from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd'));
	
	
drop table CMG.PRT_CMG_RGL_o;
create table CMG.PRT_CMG_RGL_o Stored As Parquet as 
select  cod_pdr,  
mese_comp, 
from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_comp,
"E" as tipo_lettura,
mot_rett_lett as Motivazione
from CMG.PRT_CMG_RGL_p
where
from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd'));
	
drop table CMG.PRT_CMG_RMV_o;
create table CMG.PRT_CMG_RMV_o Stored As Parquet as 
select  cod_pdr,  
annomese_riferimento, 
from_unixtime(unix_timestamp(dt_caricamento , 'yyyy-MM-dd HH:mm:ss')) as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
"E" as tipo_lettura,
mot_rett_lett as Motivazione
from CMG.PRT_CMG_RMV_p
where
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy'))  >= from_unixtime(unix_timestamp(add_months(from_unixtime(unix_timestamp()),-13),'yyyy-MM-dd'));
	
	



