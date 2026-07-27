alter table misuregas.misure_storic drop partition(tipo_flusso<>'');

drop TABLE CMG.PRT_CMG_TML_o;
Create Table CMG.PRT_CMG_TML_o  Stored As Parquet
as
select      cod_pdr,
            annomese_riferimento,
            date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
            tipo_lettura            
from CMG_GAS.PRT_CMG_TML_P
where tipo_lettura ="E" and cast(annomese as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;

drop table CMG.PRT_CMG_RML_o; 
Create Table CMG.PRT_CMG_RML_o Stored As Parquet
as
select        cod_pdr,
            annomese_riferimento,
            date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
            mot_rett_lett as Motivazione    
from CMG_GAS.PRT_CMG_RML_p
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;

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
    where cast(date_format(D_DATA_MIS_EFF_TS,'yyyyMM') as int)  >= cast(date_format(date_sub(current_date(),632),'yyyyMM') as int)
    and t_tipo_lettura='E';
	
drop Table     CMG.PRT_CMG_TAL_o;
Create Table CMG.PRT_CMG_TAL_o Stored As Parquet
as 
select
    cod_pdr,
    annomese_riferimento,
    date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
    let_tot_prel,
    from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy')) as data_lettura    
from CMG_GAS.PRT_CMG_TAL_p
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;

drop table CMG.PRT_CMG_TAV_o;
Create Table CMG.PRT_CMG_TAV_o Stored As Parquet
as 
select
    cod_pdr,
    annomese_riferimento,
    date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
    let_tot_prel,
    from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy')) as data_lettura    
from CMG_GAS.PRT_CMG_TAV_p
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int);

drop table CMG.PRT_CMG_TGL_o;
create table CMG.PRT_CMG_TGL_o Stored As Parquet as
select  cod_pdr,
concat(substr(TGL.mese_comp,3,4),substr(TGL.mese_comp,1,2)) mese_comp ,
date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
tipo_lettura
from CMG_GAS.PRT_CMG_TGL_p TGL
where tipo_lettura ='E' AND 
cast((date_format(from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')),'yyyyMM')) as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;

drop table CMG.PRT_CMG_RGL_o;
create table CMG.PRT_CMG_RGL_o Stored As Parquet as 
select  cod_pdr,  
concat(substr(PRT_CMG_RGL_p.mese_comp,3,4),substr(PRT_CMG_RGL_p.mese_comp,1,2)) mese_comp ,
date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
let_tot_prel,
date_format(data_racc,'yyyy-MM-dd HH:mm:ss')data_comp,
"E" as tipo_lettura,
mot_rett_lett as Motivazione
from CMG_GAS.PRT_CMG_RGL_p
where cast((date_format(data_racc,'yyyyMM')) as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;
	  
	  
drop table CMG.PRT_CMG_RMV_o;
create table CMG.PRT_CMG_RMV_o Stored As Parquet as 
select  cod_pdr,  
annomese_riferimento, 
date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
"E" as tipo_lettura,
mot_rett_lett as Motivazione
from CMG_GAS.PRT_CMG_RMV_p
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),632),'yyyyMM') as int) ;


drop table misuregas.letture_tml;
create table misuregas.letture_tml  Stored As Parquet as 
  select  cod_pdr,
            annomese_riferimento,
            dt_caricamento,
            let_tot_prel,
            data_racc,
            tipo_lettura,
            codice_fornitura,
            'TML' as Flusso            
    from CMG.PRT_CMG_TML_o as PRT_CMG_TML
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TML.cod_pdr
    where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
	      cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
		  NVL(forniture_gas.t_codice_pdr,'') <> '';
		  
drop table misuregas.letture_rml;
create table misuregas.letture_rml  Stored As Parquet as 
    select  cod_pdr,  
            annomese_riferimento, 
            dt_caricamento,
            let_tot_prel,
            data_racc,
            "E" as tipo_lettura,
            codice_fornitura,
            'RML' as Flusso,
            Motivazione
    from CMG.PRT_CMG_RML_o as PRT_CMG_RML
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RML.cod_pdr  
  where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
	    cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and
        NVL(forniture_gas.t_codice_pdr,'') <> '';
		
drop table misuregas.letture_vtg;
Create Table misuregas.letture_vtg Stored AS Parquet as 
SELECT distinct PRT_VTG6.cod_pdr,
 PRT_VTG6.annomese_riferimento, 
             PRT_VTG6.dt_caricamento,
            PRT_VTG6.let_tot_prel,
            PRT_VTG6.data_racc,
            PRT_VTG6.tipo_lettura,
            codice_fornitura,
            "VTG6" as flusso,
	    PRT_CMG_RMV.cod_pdr cod_pdr_rmv
            FROM SWITCH_GAS.PRT_VTG6_o as PRT_VTG6
            inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_VTG6.cod_pdr  
	    left outer join  CMG.PRT_CMG_RMV_o as PRT_CMG_RMV on  PRT_VTG6.cod_pdr=PRT_CMG_RMV.cod_pdr and PRT_VTG6.annomese_riferimento=PRT_CMG_RMV.annomese_riferimento
    where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
		  
drop table misuregas.letture_tal;
create table misuregas.letture_tal  Stored As Parquet as
select  PRT_CMG_TAL.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAL' as Flusso
from CMG.PRT_CMG_TAL_o as PRT_CMG_TAL
inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAL.cod_pdr
where cast(date_format(data_lettura,'yyyyMMdd') as int) >=data_inizio_fornitura_num and
      cast(date_format(data_lettura,'yyyyMMdd') as int) <=data_fine_fornitura_num and
      NVL(forniture_gas.t_codice_pdr,'') <> '';
	  
drop table misuregas.letture_tav;
create table misuregas.letture_tav Stored As Parquet as
select  PRT_CMG_TAV.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAV' as Flusso
from CMG.PRT_CMG_TAV_o as PRT_CMG_TAV
inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAV.cod_pdr
where cast(date_format(data_lettura,'yyyyMMdd') as int) >=data_inizio_fornitura_num and
      cast(date_format(data_lettura,'yyyyMMdd') as int) <=data_fine_fornitura_num and
      NVL(forniture_gas.t_codice_pdr,'') <> '';
	  
drop table misuregas.letture_tgl;
create table misuregas.letture_tgl Stored As Parquet as 
    select  cod_pdr,  
            mese_comp, 
            dt_caricamento,
            let_tot_prel,
            data_comp,
            tipo_lettura,
            codice_fornitura,
            'TGL' as Flusso,
            '' as Motivazione
    from CMG.PRT_CMG_TGL_o as PRT_CMG_TGL
     inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TGL.cod_pdr
    where cast(date_format(data_comp,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_comp,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
		  
drop table misuregas.letture_rgl;
create table misuregas.letture_rgl Stored As Parquet as 
    select  cod_pdr,  
            mese_comp, 
            dt_caricamento,
            let_tot_prel,
            data_comp,
            "E" as tipo_lettura,
            codice_fornitura,
            'RGL' as Flusso,
            Motivazione
    from CMG.PRT_CMG_RGL_o as PRT_CMG_RGL
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RGL.cod_pdr  
    where cast(date_format(data_comp,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_comp,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
		  
drop table misuregas.letture_rmv;
create table misuregas.letture_rmv  Stored As Parquet as 
  select  cod_pdr,
            annomese_riferimento,
            dt_caricamento,
            let_tot_prel,
            data_comp,
            tipo_lettura,
            codice_fornitura,
            'RMV' as Flusso,
            Motivazione            
    from CMG.PRT_CMG_RMV_o as PRT_CMG_RMV
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RMV.cod_pdr
    where cast(date_format(data_comp,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_comp,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO misuregas.misure_storic PARTITION(tipo_flusso,annomese)
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_tml 
UNION ALL 
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_rml 
UNION ALL
select  cod_pdr,cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,'' data_lettura,data_racc,tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_vtg 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_tal 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel,'' data_comp,data_lettura,'' data_racc,'' tipo_lettura,'' Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_tav   
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,mese_comp annomese
from misuregas.letture_tgl   
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,mese_comp annomese_riferimento, mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,mese_comp annomese
from misuregas.letture_rgl 
UNION ALL
select  cod_pdr,'' as cod_pdr_rmv,codice_fornitura,annomese_riferimento,'' mese_comp,
dt_caricamento,let_tot_prel, data_comp,'' data_lettura,'' data_racc,tipo_lettura,Motivazione,
Flusso as tipo_flusso,annomese_riferimento annomese
from misuregas.letture_rmv ;

DROP TABLE IF EXISTS misuregas.letture_tml;
DROP TABLE IF EXISTS misuregas.letture_rml;
DROP TABLE IF EXISTS misuregas.letture_vtg;
DROP TABLE IF EXISTS misuregas.letture_tal;
DROP TABLE IF EXISTS misuregas.letture_tav;
DROP TABLE IF EXISTS misuregas.letture_tgl;
DROP TABLE IF EXISTS misuregas.letture_rgl;
DROP TABLE IF EXISTS misuregas.letture_rmv; 

alter table misuregas.misure_storic drop partition(tipo_flusso<>'',annomese<201901);
