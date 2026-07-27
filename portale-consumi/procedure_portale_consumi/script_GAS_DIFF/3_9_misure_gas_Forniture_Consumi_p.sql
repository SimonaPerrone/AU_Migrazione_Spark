drop table misuregas.consumi_result_mm;
create table misuregas.consumi_result_mm  Stored As Parquet as
    select *,CASE tipo_flusso WHEN 'TML' THEN '0' ELSE '1' END as priority 
	from misuregas.misure_storic where tipo_flusso in('TML','RML') ;

	


drop table misuregas.consumi_result_mm2;
create table misuregas.consumi_result_mm2  Stored As Parquet as
    select distinct
            lu.cod_pdr,
            t4.annomese_riferimento,
            lu.dt_caricamento,
            lu.let_tot_prel,
            lu.data_racc,
            lu.tipo_lettura,
            lu.codice_fornitura,
            lu.tipo_flusso Flusso,'AF' as Tipo from misuregas.consumi_result_mm as lu
inner join (
    select cod_pdr,annomese_riferimento, Max(CONCAT(priority,data_racc,dt_caricamento)) as dt_caricamento_racc
    from
    (
    select cod_pdr,
   cast(CONCAT(substr(data_racc,1,4),substr(data_racc,6,2)) as int) as annomese_riferimento,
   data_racc,dt_caricamento,priority
    from misuregas.consumi_result_mm
    ) as T1
    group by T1.cod_pdr,T1.annomese_riferimento
     ) as t4 on t4.cod_pdr = lu.cod_pdr and t4.dt_caricamento_racc = CONCAT(lu.priority,lu.data_racc,lu.dt_caricamento)
     union all select *, 'V' as Tipo from
     (
      SELECT distinct PRT_VTG6.cod_pdr,
      PRT_VTG6.annomese_riferimento, PRT_VTG6.dt_caricamento,
      PRT_VTG6.let_tot_prel,PRT_VTG6.data_racc,
      PRT_VTG6.tipo_lettura,PRT_VTG6.codice_fornitura,
      PRT_VTG6.tipo_flusso flusso
      FROM misuregas.misure_storic PRT_VTG6 where tipo_flusso='VTG6'  AND PRT_VTG6.cod_pdr_rmv is null
	  
     ) as volture
union all select *, 'V' as Tipo from
(
select  PRT_CMG_RMV.cod_pdr,PRT_CMG_RMV.annomese_riferimento,
        PRT_CMG_RMV.dt_caricamento,
        PRT_CMG_RMV.let_tot_prel,
        PRT_CMG_RMV.data_comp as data_racc,
        PRT_CMG_RMV.tipo_lettura,
        PRT_CMG_RMV.codice_fornitura,
        PRT_CMG_RMV.tipo_flusso flusso
    from misuregas.misure_storic PRT_CMG_RMV where tipo_flusso='RMV' 
	  
) as rmv;



drop table misuregas.consumi_mensili_tmp;
create table misuregas.consumi_mensili_tmp Stored As Parquet as
  select cod_pdr,
       dt_caricamento,
       data_racc,
       annomese_riferimento,
       tipo_lettura,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as let_tot_prel_next,
       cast (lag(let_tot_prel,1) over (partition by cod_pdr  order by data_racc)   as double) as let_tot_prel_prev,
       Tipo,
       Flusso
    from misuregas.consumi_result_mm2;



drop table misuregas.misure_gas_portale_af;
create table misuregas.misure_gas_portale_af Stored As Parquet as
    select cod_pdr,
           annomese_riferimento as competenza_consumi_af,
           data_racc as data_lettura_af,
           Flusso as tipo_misura_af,
           n_id_fornitura,
           let_tot_prel_next as lettura_mese_af,
           nvl((let_tot_prel_next - let_tot_prel_prev),0.0) as delta_misure_af
      from misuregas.consumi_mensili_tmp
      where Tipo='AF';


--autolettura


drop table misuregas.union_autoletture;
create table misuregas.union_autoletture Stored As Parquet as
select  PRT_CMG_TAL.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    tipo_flusso as Flusso
from (select * from misuregas.misure_storic where tipo_flusso='TAL' )  as PRT_CMG_TAL
left join misuregas.misure_gas_portale_af on misure_gas_portale_af.cod_pdr=PRT_CMG_TAL.cod_pdr
and misure_gas_portale_af.n_id_fornitura= PRT_CMG_TAL.codice_fornitura
and  misure_gas_portale_af.competenza_consumi_af=PRT_CMG_TAL.annomese_riferimento
where misure_gas_portale_af.competenza_consumi_af is null
UNION ALL
select  PRT_CMG_TAV.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    tipo_flusso as Flusso
from (select * from misuregas.misure_storic where tipo_flusso='TAV' ) as PRT_CMG_TAV
left join misuregas.misure_gas_portale_af on misure_gas_portale_af.cod_pdr=PRT_CMG_TAV.cod_pdr
and misure_gas_portale_af.n_id_fornitura= PRT_CMG_TAV.codice_fornitura
and  misure_gas_portale_af.competenza_consumi_af=PRT_CMG_TAV.annomese_riferimento
where misure_gas_portale_af.competenza_consumi_af is null;


drop table misuregas.union_autoletture2;
create table misuregas.union_autoletture2 Stored As Parquet as
select distinct lu.* from misuregas.union_autoletture as lu
inner join
     (
    select cod_pdr,annomese_riferimento,Flusso, Max(concat(data_lettura,dt_caricamento)) as dt_caricamento_max
    from
    (
    select cod_pdr,
    CONCAT(YEAR(data_lettura),LPAD(MONTH( data_lettura),2,0)) as annomese_riferimento,data_lettura,
    dt_caricamento,Flusso
    from misuregas.union_autoletture
    ) as T1
    group by T1.cod_pdr,T1.annomese_riferimento ,T1.Flusso
     ) as t4 on t4.cod_pdr = lu.cod_pdr  AND t4.Flusso = lu.Flusso and t4.dt_caricamento_max = CONCAT(lu.data_lettura,lu.dt_caricamento) ;


drop table misuregas.consumi_autoletture;
create table misuregas.consumi_autoletture Stored As Parquet as
  select cod_pdr,
       dt_caricamento,
       annomese_riferimento as competenza_consumi_autoletture,
       data_lettura as data_lettura_autoletture,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as lettura_mese_autoletture,
       Flusso as tipo_lettura_autoletture
    from misuregas.union_autoletture2;



drop table misuregas.letture_union;
    create table misuregas.letture_union Stored As Parquet as
        select *,CASE tipo_flusso WHEN 'TGL' THEN '0' ELSE '1' END as priority
		from misuregas.misure_storic where tipo_flusso in('TGL','RGL')   ; 

drop table misuregas.letture_union2;
create table misuregas.letture_union2 Stored As Parquet
as
select distinct lu.* from misuregas.letture_union as lu
inner join (
        select  cod_pdr, data_comp,  MAX(CONCAT(priority,data_comp,dt_caricamento)) as dt_caricamento_max from misuregas.letture_union
        group by cod_pdr, data_comp
    ) as t3 on t3.cod_pdr = lu.cod_pdr and  t3.dt_caricamento_max = CONCAT(lu.priority,lu.data_comp,lu.dt_caricamento);


drop table misuregas.letture_union3;
Create Table misuregas.letture_union3 Stored as Parquet
     As
     select cod_pdr,mese_comp,dt_caricamento,let_tot_prel,data_comp,tipo_lettura,codice_fornitura,flusso,'G' as tipo from
     (SELECT cod_pdr,mese_comp,dt_caricamento,let_tot_prel,data_comp,tipo_lettura,codice_fornitura,tipo_flusso as flusso,motivazione from
     misuregas.letture_union2
     ) AS t
     union all
             SELECT consumi_result_mm2.cod_pdr,
                 consumi_result_mm2.annomese_riferimento as mese_comp,
                dt_caricamento,
                 let_tot_prel,
                  data_racc as  data_comp,
              tipo_lettura,
                 consumi_result_mm2.codice_fornitura,
                 flusso,
                 'M' as tipo
           from misuregas.consumi_result_mm2
           left join
           (
           select distinct cod_pdr,
             CONCAT(YEAR(data_comp),LPAD(MONTH( data_comp),2,0)) as annomese_riferimento,codice_fornitura from
             misuregas.letture_union2
           ) as T1
           on consumi_result_mm2.cod_pdr=T1.cod_pdr and
           consumi_result_mm2.annomese_riferimento=T1.annomese_riferimento and
           consumi_result_mm2.codice_fornitura=T1.codice_fornitura
           where T1.cod_pdr is null;



drop table misuregas.consumi_giornalieri_tmp;
create table misuregas.consumi_giornalieri_tmp Stored As Parquet as
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
       from misuregas.letture_union3;


    drop table misuregas.misure_gas_portale_gg;
    create table misuregas.misure_gas_portale_gg Stored As Parquet as
        select cod_pdr,
               mese_comp as competenza_consumi_gg,
               data_comp as data_lettura_gg,
               Flusso as tipo_misura_gg,
               n_id_fornitura,
               let_tot_prel_next as lettura_gg,
               nvl((let_tot_prel_next - let_tot_prel_prev),0.0) as delta_misure_gg
           from misuregas.consumi_giornalieri_tmp
        where tipo='G';



--misure mensili

 drop table misuregas.consumi_result_fal;
create table misuregas.consumi_result_fal Stored As Parquet as
 select cod_pdr,
       dt_caricamento,
       data_racc,
       annomese_riferimento,
       tipo_lettura,
       codice_fornitura as n_id_fornitura,
       cast(let_tot_prel as double) as let_tot_prel_next,
       flusso
    from misuregas.consumi_result_mm2

    union all

    select cod_pdr,
       dt_caricamento,
       data_lettura_autoletture as data_racc,
       competenza_consumi_autoletture as  annomese_riferimento,
         "E" as tipo_lettura,
        n_id_fornitura,
        cast(lettura_mese_autoletture as double)  as let_tot_prel_next, tipo_lettura_autoletture as flusso
        from misuregas.consumi_autoletture;



drop table misuregas.consumi_mensili_fal;
create table misuregas.consumi_mensili_fal Stored As Parquet as
  select cod_pdr,
       dt_caricamento,
       data_racc,
       annomese_riferimento,
       tipo_lettura,
       n_id_fornitura,
       let_tot_prel_next,
       cast (lag(let_tot_prel_next,1) over (partition by cod_pdr, n_id_fornitura  order by data_racc)   as double) as let_tot_prel_prev,
       flusso
from misuregas.consumi_result_fal;


        drop table misuregas.misure_gas_portale_fal;
create table misuregas.misure_gas_portale_fal Stored As Parquet as
    select cod_pdr,
           annomese_riferimento as competenza_consumi_af,
           data_racc as data_lettura_af,
           flusso as tipo_misura_af,
           n_id_fornitura,
           let_tot_prel_next as lettura_mese_af,
           nvl((let_tot_prel_next - let_tot_prel_prev),0.0) as delta_misure_af

      from misuregas.consumi_mensili_fal;



      drop Table misuregas.misure_gas_portale_mensili;
          Create Table misuregas.misure_gas_portale_mensili  Stored As Parquet as

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
            sum(nvl(delta_misure_gg,0.0)) over(partition by misure_gas_portale_gg.n_id_fornitura,competenza_consumi_gg) as  delta_misure_mmmm
           from misuregas.misure_gas_portale_gg
        left join misuregas.misure_gas_portale_fal
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
           nvl(delta_misure_af,0.0)  as  delta_misure_mmmm
      from misuregas.misure_gas_portale_fal
        ) as af;

