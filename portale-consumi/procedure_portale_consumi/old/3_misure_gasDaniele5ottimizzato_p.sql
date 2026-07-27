
--altra frequenza

drop table au_test.letture_tml;
create table au_test.letture_tml  Stored As Parquet as 
  select  cod_pdr,
            annomese_riferimento,
            dt_caricamento,
            let_tot_prel,
            data_racc,
            tipo_lettura,
            codice_fornitura,
            'TML' as Flusso            
    from CMG.PRT_CMG_TML_o as PRT_CMG_TML
    inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TML.cod_pdr
    where 
          data_racc>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_racc <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';


        
drop table au_test.letture_rml;
create table au_test.letture_rml  Stored As Parquet as 
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
    inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RML.cod_pdr  
  where 
          data_racc>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_racc <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';




drop table au_test.consumi_result_mm;
  create table au_test.consumi_result_mm  Stored As Parquet as
    select *,'' as Motivazione from au_test.letture_tml UNION ALL select * from au_test.letture_rml;




drop table au_test.letture_vtg;
Create Table au_test.letture_vtg Stored AS Parquet as 
SELECT distinct PRT_VTG6.cod_pdr,
 annomese_riferimento, 
             dt_caricamento,
            let_tot_prel,
             data_racc,
            tipo_lettura,
            codice_fornitura,
            "VTG6" as flusso
            FROM SWITCH_GAS.PRT_VTG6_o as PRT_VTG6
            inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_VTG6.cod_pdr  
    where 
    data_racc>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_racc <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';


    
    
drop table au_test.consumi_result_mm2;
create table au_test.consumi_result_mm2  Stored As Parquet as
    select distinct 
            lu.cod_pdr,  
            t4.annomese_riferimento, 
            lu.dt_caricamento,
            lu.let_tot_prel,
            lu.data_racc,
            lu.tipo_lettura,
            lu.codice_fornitura,
            lu.Flusso,'AF' as Tipo from au_test.consumi_result_mm as lu
inner join (    
    select cod_pdr,annomese_riferimento, Max(CONCAT(data_racc,dt_caricamento)) as dt_caricamento_racc
    from
    (
    select cod_pdr,
   cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) as annomese_riferimento,
   data_racc,dt_caricamento
    from au_test.consumi_result_mm 
    ) as T1    
    group by T1.cod_pdr,T1.annomese_riferimento         
     ) as t4 on t4.cod_pdr = lu.cod_pdr and t4.dt_caricamento_racc = CONCAT(lu.data_racc,lu.dt_caricamento) 
     union all select *, 'V' as Tipo from
     (
SELECT distinct PRT_VTG6.cod_pdr,
 PRT_VTG6.annomese_riferimento, 
             PRT_VTG6.dt_caricamento,
            PRT_VTG6.let_tot_prel,
             PRT_VTG6.data_racc,
            PRT_VTG6.tipo_lettura,
            codice_fornitura,
            "VTG6" as flusso
            FROM SWITCH_GAS.PRT_VTG6_o as PRT_VTG6
            inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_VTG6.cod_pdr
            left join     CMG.PRT_CMG_RMV_o as PRT_CMG_RMV on     PRT_VTG6.cod_pdr=PRT_CMG_RMV.cod_pdr
            and PRT_VTG6.annomese_riferimento=PRT_CMG_RMV.annomese_riferimento
    where 
    data_racc>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_racc <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '' and PRT_CMG_RMV.cod_pdr is null

) as volture
union all select *, 'V' as Tipo from
(
select  cod_pdr,
            PRT_CMG_RMV.annomese_riferimento,
            PRT_CMG_RMV.dt_caricamento,
            PRT_CMG_RMV.let_tot_prel,
            PRT_CMG_RMV.data_comp as data_racc,
            PRT_CMG_RMV.tipo_lettura,
            codice_fornitura,
            'RMV' as Flusso                
    from CMG.PRT_CMG_RMV_o as PRT_CMG_RMV
    inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RMV.cod_pdr
    where 
          data_comp>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_comp <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> ''
) as rmv;

    
    

drop table au_test.consumi_mensili_tmp;
create table au_test.consumi_mensili_tmp Stored As Parquet as 
  select cod_pdr, 
       dt_caricamento,
       data_racc, 
       annomese_riferimento,
       tipo_lettura,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as let_tot_prel_next,
       cast (lag(let_tot_prel,1) over (partition by cod_pdr, codice_fornitura  order by data_racc)   as double) as let_tot_prel_prev,
       Tipo,
       Flusso
    from au_test.consumi_result_mm2;

    

    
drop table misure.misure_gas_portale_af;
create table misure.misure_gas_portale_af Stored As Parquet as 
    select cod_pdr, 
           annomese_riferimento as competenza_consumi_af,
           data_racc as data_lettura_af,
           Flusso as tipo_misura_af,
           n_id_fornitura,
           let_tot_prel_next as lettura_mese_af,
           (let_tot_prel_next - let_tot_prel_prev) as delta_misure_af           
      from au_test.consumi_mensili_tmp
      where Tipo='AF';


--autolettura


drop table au_test.letture_tal;
create table au_test.letture_tal  Stored As Parquet as
select  PRT_CMG_TAL.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAL' as Flusso
from CMG.PRT_CMG_TAL_o as PRT_CMG_TAL
inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAL.cod_pdr
left join misure.misure_gas_portale_af on misure_gas_portale_af.cod_pdr=PRT_CMG_TAL.cod_pdr
and misure_gas_portale_af.n_id_fornitura= forniture_gas.codice_fornitura
and  misure_gas_portale_af.competenza_consumi_af=PRT_CMG_TAL.annomese_riferimento
where         
    misure_gas_portale_af.competenza_consumi_af is null    
    and      data_lettura >=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_lettura<=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';


drop table au_test.letture_tav;
create table au_test.letture_tav Stored As Parquet as
select  PRT_CMG_TAV.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAV' as Flusso
from CMG.PRT_CMG_TAV_o as PRT_CMG_TAV
inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAV.cod_pdr
left join misure.misure_gas_portale_af on misure_gas_portale_af.cod_pdr=PRT_CMG_TAV.cod_pdr
and misure_gas_portale_af.n_id_fornitura= forniture_gas.codice_fornitura
and  misure_gas_portale_af.competenza_consumi_af=PRT_CMG_TAV.annomese_riferimento
where         
    misure_gas_portale_af.competenza_consumi_af is null    
    and      data_lettura >=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_lettura<=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';


drop table au_test.union_autoletture;
create table au_test.union_autoletture Stored As Parquet as
  select * from au_test.letture_tav UNION ALL select * from au_test.letture_tal;



    
drop table au_test.union_autoletture2;
create table au_test.union_autoletture2 Stored As Parquet as
select distinct lu.* from au_test.union_autoletture as lu
inner join
     (    
    select cod_pdr,annomese_riferimento, Max(dt_caricamento) as dt_caricamento
    from
    (
    select cod_pdr,
    CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) as annomese_riferimento,
    dt_caricamento
    from au_test.union_autoletture 
    ) as T1    
    group by T1.cod_pdr,T1.annomese_riferimento         
     ) as t4 on t4.cod_pdr = lu.cod_pdr and t4.dt_caricamento = lu.dt_caricamento; 
    

drop table misure.consumi_autoletture;
create table misure.consumi_autoletture Stored As Parquet as 
  select cod_pdr, 
       dt_caricamento,
       annomese_riferimento as competenza_consumi_autoletture,
       data_lettura as data_lettura_autoletture,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as lettura_mese_autoletture,
       Flusso as tipo_lettura_autoletture
    from au_test.union_autoletture2;
    
    
    
----- Giornaliero

drop table au_test.letture_tgl;
create table au_test.letture_tgl Stored As Parquet as 
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
     inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TGL.cod_pdr
    where 
    data_comp  >=datetime_inizio_fornitura 
    and (NVL(data_fine_fornitura,'') = '' or data_comp <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';



drop table au_test.letture_rgl;
create table au_test.letture_rgl Stored As Parquet as 
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
    inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RGL.cod_pdr  
    where 
    data_comp  >=datetime_inizio_fornitura 
    and (NVL(data_fine_fornitura,'') = '' or data_comp <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';

    

drop table au_test.letture_union;
    create table au_test.letture_union Stored As Parquet as 
        select * from au_test.letture_rgl UNION ALL select * from au_test.letture_tgl;
        
drop table au_test.letture_union2;
create table au_test.letture_union2 Stored As Parquet
as
select distinct lu.* from au_test.letture_union as lu
inner join (
        select  cod_pdr, data_comp,  MAX(dt_caricamento) as dt_caricamento from au_test.letture_union 
        group by cod_pdr, data_comp
    ) as t3 on t3.cod_pdr = lu.cod_pdr and t3.data_comp = lu.data_comp and t3.dt_caricamento = lu.dt_caricamento;

    

drop table au_test.letture_union3;
Create Table au_test.letture_union3 Stored as Parquet
     As
     select cod_pdr,mese_comp,dt_caricamento,let_tot_prel,data_comp,tipo_lettura,codice_fornitura,flusso,'G' as tipo from
     (SELECT cod_pdr,mese_comp,dt_caricamento,let_tot_prel,data_comp,tipo_lettura,codice_fornitura,flusso,motivazione from
     au_test.letture_union2
     ) AS t
     union all
             SELECT consumi_result_mm2.cod_pdr,
                 consumi_result_mm2.annomese_riferimento as mese_comp,
                dt_caricamento,
                 let_tot_prel,
                  data_racc as  data_comp,
              tipo_lettura,
                 consumi_result_mm2.codice_fornitura,
                 Flusso as flusso,
                 'M' as tipo
           from au_test.consumi_result_mm2
           left join
           (
           select distinct cod_pdr,
             CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) as annomese_riferimento,codice_fornitura from
             au_test.letture_union2
           ) as T1
           on consumi_result_mm2.cod_pdr=T1.cod_pdr and
           consumi_result_mm2.annomese_riferimento=T1.annomese_riferimento and
           consumi_result_mm2.codice_fornitura=T1.codice_fornitura
           where T1.cod_pdr is null;



    drop table au_test.consumi_giornalieri_tmp;
create table au_test.consumi_giornalieri_tmp Stored As Parquet as 
    select cod_pdr, 
       dt_caricamento,
       mese_comp, 
       data_comp,
       tipo_lettura as tipo_misura_mg,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as let_tot_prel_next,
       cast (lag(let_tot_prel,1) over (partition by cod_pdr, codice_fornitura  order by data_comp)   as double) as let_tot_prel_prev,
        tipo,
        Flusso        
       from au_test.letture_union3;
    

    drop table misure.misure_gas_portale_gg;
    create table misure.misure_gas_portale_gg Stored As Parquet as 
        select cod_pdr, 
               mese_comp as competenza_consumi_gg,
               data_comp as data_lettura_gg,
               Flusso as tipo_misura_gg,
               n_id_fornitura,
               let_tot_prel_next as lettura_gg,
               (let_tot_prel_next - let_tot_prel_prev) as delta_misure_gg               
           from au_test.consumi_giornalieri_tmp
        where tipo='G';
        

    
--misure mensili
 
 drop table misure.consumi_result_fal;
create table misure.consumi_result_fal Stored As Parquet as 
 select cod_pdr, 
       dt_caricamento,
       data_racc, 
       annomese_riferimento,
       tipo_lettura,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as let_tot_prel_next,
       flusso       
    from au_test.consumi_result_mm2 
   
    union all 

    select cod_pdr, 
       dt_caricamento,      
       data_lettura_autoletture as data_racc,
       competenza_consumi_autoletture as  annomese_riferimento,
         "E" as tipo_lettura,
        n_id_fornitura,
        cast(lettura_mese_autoletture as double)  as let_tot_prel_next, tipo_lettura_autoletture as flusso
        from misure.consumi_autoletture;
        


drop table au_test.consumi_mensili_fal; 
create table au_test.consumi_mensili_fal Stored As Parquet as 
  select cod_pdr, 
       dt_caricamento,
       data_racc, 
       annomese_riferimento,
       tipo_lettura,
       n_id_fornitura,
       let_tot_prel_next,
       cast (lag(let_tot_prel_next,1) over (partition by cod_pdr, n_id_fornitura  order by data_racc)   as double) as let_tot_prel_prev,
       flusso
from misure.consumi_result_fal;
    

        drop table misure.misure_gas_portale_fal;
create table misure.misure_gas_portale_fal Stored As Parquet as 
    select cod_pdr, 
           annomese_riferimento as competenza_consumi_af,
           data_racc as data_lettura_af,
           flusso as tipo_misura_af,
           n_id_fornitura,
           let_tot_prel_next as lettura_mese_af,
           (let_tot_prel_next - let_tot_prel_prev) as delta_misure_af
           
      from au_test.consumi_mensili_fal;



      drop Table misure.misure_gas_portale_mensili;
          Create Table misure.misure_gas_portale_mensili  Stored As Parquet as
          
          Select *
          From
          (
              select 
            misure_gas_portale_gg.cod_pdr, 
            misure_gas_portale_gg.n_id_fornitura,            
            competenza_consumi_gg as competenza_consumi_mmmm,           
            tipo_misura_gg as tipo_misura_mmmm,
             data_lettura_gg as data_lettura_mmmm,
            last_value(lettura_gg) over(partition by misure_gas_portale_gg.n_id_fornitura,competenza_consumi_gg) as lettura_misure_mmmm,
            sum(delta_misure_gg) over(partition by misure_gas_portale_gg.n_id_fornitura,competenza_consumi_gg) as  delta_misure_mmmm
           from misure.misure_gas_portale_gg 
        left join misure.misure_gas_portale_fal
        on misure_gas_portale_gg.cod_pdr=misure_gas_portale_fal.cod_pdr
        and misure_gas_portale_gg.competenza_consumi_gg=misure_gas_portale_fal.competenza_consumi_af
        and misure_gas_portale_gg.n_id_fornitura=misure_gas_portale_fal.n_id_fornitura
        
        where misure_gas_portale_fal.cod_pdr is null
        ) as gg
            
          union ALL Select * from
          (
          select 
          cod_pdr, 
           n_id_fornitura,
           competenza_consumi_af as competenza_consumi_mmmm,     
            tipo_misura_af as tipo_misura_mmmm,            
           data_lettura_af  as data_lettura_mmmm,     
           lettura_mese_af as lettura_misure_mmmm,
           delta_misure_af  as  delta_misure_mmmm
      from misure.misure_gas_portale_fal
        ) as af;
        
    



    
    
--RMV
drop table au_test.letture_rmv;
create table au_test.letture_rmv  Stored As Parquet as 
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
    inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RMV.cod_pdr
    where 
          data_comp>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_comp <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '';
    
----Volture
Drop Table misure.VoltureGas;
Create Table misure.VoltureGas Stored as Parquet as 
select * 
From
(
SELECT distinct PRT_VTG6.cod_pdr,
PRT_VTG6.annomese_riferimento as competenza_consumi_v,
PRT_VTG6.dt_caricamento  as data_lettura_v,
"VTG6" as tipo_misure_v,
forniture_gas.codice_fornitura as codice_fornitura_v,                                           
PRT_VTG6.let_tot_prel     as lettura_v                                   
FROM SWITCH_GAS.PRT_VTG6_o as PRT_VTG6
inner join misure.forniture_gas on forniture_gas.t_codice_pdr=PRT_VTG6.cod_pdr
left join au_test.letture_rmv  on PRT_VTG6.cod_pdr=letture_rmv.cod_pdr
and PRT_VTG6.annomese_riferimento=letture_rmv.annomese_riferimento
where data_racc>=datetime_inizio_fornitura
    and (NVL(data_fine_fornitura,'') = '' or data_racc <=datetime_fine_fornitura)
    and NVL(forniture_gas.t_codice_pdr,'') <> '' and letture_rmv.cod_pdr is null
) as V
union all select * from
(
select  cod_pdr,
annomese_riferimento as competenza_consumi_v,
dt_caricamento  as data_lettura_v,
"RMV" as tipo_misure_v,
codice_fornitura as codice_fornitura_v,                                           
let_tot_prel     as lettura_v      
from   au_test.letture_rmv
) as RMV;

