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
        select *,'1' priority from misuregas.letture_rgl UNION ALL select *,'0' priority from misuregas.letture_tgl;

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
     (SELECT cod_pdr,mese_comp,dt_caricamento,let_tot_prel,data_comp,tipo_lettura,codice_fornitura,flusso,motivazione from
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
                 Flusso as flusso,
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



----Volture
Drop Table misuregas.VoltureGas;
Create Table misuregas.VoltureGas Stored as Parquet as
select *
From
(
SELECT distinct PRT_VTG6.cod_pdr,
PRT_VTG6.annomese_riferimento as competenza_consumi_v,
PRT_VTG6.dt_caricamento  as data_lettura_v,
PRT_VTG6.flusso as tipo_misure_v,
PRT_VTG6.codice_fornitura as codice_fornitura_v,
PRT_VTG6.let_tot_prel     as lettura_v
FROM misuregas.letture_vtg  PRT_VTG6
where  PRT_VTG6.cod_pdr_rmv is null
) as V
union all select * from
(
select  cod_pdr,
annomese_riferimento as competenza_consumi_v,
dt_caricamento  as data_lettura_v,
"RMV" as tipo_misure_v,
codice_fornitura as codice_fornitura_v,
let_tot_prel     as lettura_v
from   misuregas.letture_rmv
) as RMV;
