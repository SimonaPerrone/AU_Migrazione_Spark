
-- Tabella per itegrazione codici istat mancanti nella tabella rcugas_massivo
drop table rcugas.rcugas_massivo_integ_p;
CREATE TABLE rcugas.rcugas_massivo_integ_p stored AS PARQUET AS
select 
    n_id_pdr,
    rcugas_massivo_p.t_codice_pdr,
    capacita_trasporto,
    mese_val_cap_trasp,
    t_cod_tipo_pdr,
    t_disalimentabilita,
    bilanciamento,
    n_id_fornitura,
    d_data_inizio_for,
    data_fine_for,
    n_id_az_udd,
    piva_udd,
    n_id_az_cc,
    piva_cc,
    n_id_cliente,
    t_partita_iva_cli,
    t_codice_fiscale_cli,
    b_cf_straniero,
    t_referente,
    t_nome_ref,
    t_cognome_ref,
    t_email_ref,
    t_telefono_ref,
    t_residenza,
    data_val_res,
    t_toponimopdr,
    t_nomestrada_pdr,
    t_civico_pdr,
    t_cap_pdr,
    (case when nvl(t_comune_istat_pdr, '') = '' then ELENCO_CODICE_ISTAT_MANC.t_codice_istat
          else rcugas_massivo_p.t_comune_istat_pdr
     end
    ) as t_comune_istat_pdr,
    t_comune_pdr,
    t_provincia_pdr,
    t_nazione_pdr,
    altro_ind_pdr,
    t_toponimo_forn,
    t_nomestrada_forn,
    t_civico_forn,
    t_cap_forn,
    t_comune_istatforn,
    t_comune_forn,
    t_provincia_forn,
    t_nazione_forn,
    altro_ind_forn,
    t_accesso_ui,
    t_tipo_fornitura,
    t_aliquota_iva,
    t_aliquota_accise,
    t_add_regionale,
    t_altre_info_imposte,
    t_matricola_misuratore,
    t_classe_misuratore,
    t_tipo_misuratore,
    t_telegestione,
    t_pre_conv,
    t_matricola_convertitore,
    n_num_cifre_convertitore,
    t_anno_fabbric_convertitore,
    t_data_inst_convertitore,
    n_coeff_correzione,
    press_misure,
    t_access_misuratore,
    n_num_cifre_misuratore,
    t_anno_fabbric_misuratore,
    t_data_inst_misuratore,
    t_misuratore_integrato,
    n_potenzialita_massima,
    n_potenzialita_tot_installata,
    n_max_prelievo_orario,
    t_erog_servizio_energ,
    t_partita_iva_gestcal,
    t_ragione_sociale_gestcal,
    t_telefono_gestcal,
    t_email_gestcal,
    t_toponimo_gestcal,
    t_nomestrada_gestcal,
    t_civico_gestcal,
    t_cap_gestcal,
    t_comune_istat_gestcal,
    t_comune_gestcal,
    t_provincia_gestcal,
    t_nazione_gestcal,
    t_indirizzo_completo,
    d_data_rif_pdr,
    d_aggiornamento_pdr,
    d_data_rif_tecn,
    d_aggiornamento_tecn,
    d_data_rif_mis,
    d_aggiornamento_mis,
    d_data_rif_forn,
    d_aggiornamento_forn,
    t_tipo_bonus,
    d_data_inizio_erog_bonus,
    d_data_fine_erog_bonus,
    d_data_rif_bonus,
    d_aggiornamento_bonus,
    d_data_aggiornamento,
    n_id_udd,
    n_id_venditore,
    t_cod_profilo,
    t_cod_cat_uso,
    t_cod_classe_prelievo,
    t_anno_termico,
    d_data_rif_prel,
    t_trattamento,
    t_toponimo_esaz,
    t_nomestrada_esaz,
    t_civico_esaz,
    t_cap_esaz,
    t_comune_istat_esaz,
    t_comune_esaz,
    t_provincia_esaz,
    t_nazione_esaz,
    altro_ind_esaz,
    t_codice_ateco,
    t_pagamento_iva,
    t_codice_ufficio,
    t_cf_intestatario_fatt,
    t_cf_straniero_fatt,
    t_piva_intestatario_fatt,
    t_nome_intestatario_fatt,
    t_cognome_intestatario_fatt,
    t_rag_soc_intestatario_fatt,
    t_anno_mese_rinn_bonus,
    d_data_inizio_bonus,
    d_data_fine_bonus,
    n_prelievo_annuo,
    t_fattore_correz_climatica,
    t_altro_ind_gestcal,
    t_tipo_op,
    t_processo 
from rcugas.rcugas_massivo_p
left join sferrara.ELENCO_CODICE_ISTAT_MANC
on rcugas_massivo_p.t_codice_pdr = ELENCO_CODICE_ISTAT_MANC.t_codice_pdr
;


-- Creazione tabella rcugas_connessioni_distr3 
Drop TABLE rcugas.rcugas_connessioni_distr3;
Create Table rcugas.rcugas_connessioni_distr3 Stored as Parquet as
    SELECT 
        DI.T_REMI_RCU, 
        DI.T_REMI, 
        DI.T_CODICE_PDR, 
        DI.N_ID_REMI, 
        DI.N_ID_PDR, 
        DI.N_ID_DISTR, 
        DI.D_DATA_INIZIO_GESTECN, 
        DI.D_DATA_INIZIO_CONN, 
        DI.D_DATA_FINE_GESTECN, 
        DI.D_DATA_FINE_CONN, 
        CL.ID_REG_CLIM

    FROM au.COD_REMI_ID_REG_CLIM CL
    join RCUGAS.RCUGAS_CONNESSIONI_DISTR2_p DI
        on DI.T_REMI = CL.T_COD_REMI
    where NVL(DI.D_DATA_FINE_CONN ,'') = '' 
;
 

-- Creazione tabella parametri caratteristici
drop table au.TAB_PARAMETRI_CARATTERISTICI_PROFILO;
create table au.TAB_PARAMETRI_CARATTERISTICI_PROFILO stored as PARQUET  as
select *,
    concat(zona_clim, cast(class_prelievo as int)) as KeyZonaClasse
from au.TAB_PARAMETRI_CARATTERISTICI_PROF_PREL
;


drop table au.ParametriPProf;
create table au.ParametriPProf stored as PARQUET  as
    select 
        TAB_FATT_TFC.data,
        case when TAB_FATT_TFC.WKR is null then cast(1 as double) else TAB_FATT_TFC.WKR end as wkr,
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.prof,
        TAB_FATT_C1.c1,
        TAB_FATT_C2C4.c2,
        TAB_FATT_C2C4.c4,
        TAB_FATT_T1.t1,
        TAB_FATT_TFC.id_reg_clim

    from au.TAB_FATT_TFC
    join au.TAB_FATT_C1
    join au.TAB_FATT_C2C4
    join au.TAB_FATT_T1
    join au.TAB_PARAMETRI_CARATTERISTICI_PROFILO
    on     TAB_FATT_TFC.data = TAB_FATT_C1.data 
       and TAB_FATT_C2C4.data = TAB_FATT_TFC.data
       and TAB_PARAMETRI_CARATTERISTICI_PROFILO.KeyZonaClasse = TAB_FATT_C1.KeyZonaClasse
       and TAB_FATT_C1.KeyDataClasse = TAB_FATT_T1.KeyDataClasse
;
       


-- 14 - Job Calcolo Profilo Standard per ogni giorno dell’anno termico
-- TAB_PARAMETRI_PERC_ANNUI_PREL_STD ==> gas_vpg
-- TAB_FATT_CLIM ==> TFC
-- TAB_PARAMETRI_CARATTERISTICI_PROF_PREL

drop table au.TAB_PROFILI_GIORN_STD_PERC;
create table au.TAB_PROFILI_GIORN_STD_PERC stored as PARQUET  as
select 
    ParametriPProf.data, 
    (
        ParametriPProf.wkr * TAB_PARAMETRI_CARATTERISTICI_PROFILO.b1 * ParametriPProf.c1 + 
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b2 * ParametriPProf.c2 +
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b3 * ParametriPProf.t1 +                                                                                
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b4 * ParametriPProf.c4 
    ) as pprofk,
    ParametriPProf.id_reg_clim,
    ParametriPProf.prof,
    ParametriPProf.wkr
from au.ParametriPProf
join au.TAB_PARAMETRI_CARATTERISTICI_PROFILO  on
--join rcugas.rcugas_connessioni_distr3 as distr on 
TAB_PARAMETRI_CARATTERISTICI_PROFILO.prof = ParametriPProf.prof
--and distr.id_reg_clim = ParametriPProf.id_reg_clim
where ParametriPProf.id_reg_clim in (select distinct rcugas_connessioni_distr3.id_reg_clim from rcugas.rcugas_connessioni_distr3 )
    and ParametriPProf.wkr is not null
union all

select 
    ParametriPProf.data, 
    (
        1 * TAB_PARAMETRI_CARATTERISTICI_PROFILO.b1 * ParametriPProf.c1 + 
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b2 * ParametriPProf.c2 +
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b3 * ParametriPProf.t1 +                                                                                
        TAB_PARAMETRI_CARATTERISTICI_PROFILO.b4 * ParametriPProf.c4 
    ) as pprofk,
    ParametriPProf.id_reg_clim,
    ParametriPProf.prof,
    1 as wkr
from au.ParametriPProf
join au.TAB_PARAMETRI_CARATTERISTICI_PROFILO  on
--join rcugas.rcugas_connessioni_distr3 as distr on 
TAB_PARAMETRI_CARATTERISTICI_PROFILO.prof = ParametriPProf.prof
--and distr.id_reg_clim = ParametriPProf.id_reg_clim
where ParametriPProf.id_reg_clim not in (select distinct rcugas_connessioni_distr3.id_reg_clim from rcugas.rcugas_connessioni_distr3 )
;


drop table au.TAB_PROFILI_GIORN_STD_PERC_FORMAT;
create table au.TAB_PROFILI_GIORN_STD_PERC_FORMAT stored as PARQUET  as
    select 
        from_unixtime(unix_timestamp(data , 'dd/MM/yyyy')) as data,
        pprofk,
        id_reg_clim,
        prof,
        wkr
    from au.TAB_PROFILI_GIORN_STD_PERC
;


-- Sono presenti tutti i campi della pdr relativo dell'ultimo anno termico
drop table rcugas.rcugas_massivo_max_annotermico_p;
create table rcugas.rcugas_massivo_max_annotermico_p stored as PARQUET  as
    select * 
    from rcugas.rcugas_massivo_p --rcugas.rcugas_massivo_integ_p
    join
    (
    select  
        max(cast(rcugas_massivo_p.t_anno_termico as int)) as ANNO_RIF, 
        rcugas_massivo_p.t_codice_pdr as PDR
    FROM rcugas.rcugas_massivo_p 
    group by rcugas_massivo_p.t_codice_pdr
    ) as t on
        t.ANNO_RIF = rcugas_massivo_p.t_anno_termico
        and t.PDR = rcugas_massivo_p.t_codice_pdr
;


-- SGIGANTE.TAB_GRADI_GIORNO_ISTAT_PL
--Job di Caricamento dei dati Settlement GAS dal RCUGAS all’ambiente HIVE
-- Si richiede una procedura che crei una tabella HIVE (TAB_RCUGAS_SETTLE_GAS_PDR) 
-- popolando la stessa a partire dai dati del RCUGAS incrociando i dati ISTAT con la tabella 
-- TAB_COMUNE_ZONA_CLIM per estrarre la zona climatica (ZONA_CLIM). Tale tabella dovrà avere le seguenti informazioni:
-- ANNO_RIF - PdR - CAT_USO – CLASSE_PREL – ZONA_CLIM - COD_PROF_STD - TRATTAMENTO – CONS_ANNUO – REGIONE_CLIM

-- Prendere il max per anno_termico
-- per calcolare la REGIONE_CLIM devo prelevare dalla tabella RCUGAS.REMI_ANAGRAFICA
drop table au.TAB_RCUGAS_SETTLE_GAS_PDR;
create table au.TAB_RCUGAS_SETTLE_GAS_PDR stored as PARQUET  as
    select DISTINCT
        cast(rcugas_massivo_p.n_prelievo_annuo as double) as CONS_ANNUO,
        t.ANNO_RIF, 
        rcugas_massivo_p.t_codice_pdr as PDR,
        substr(rcugas_massivo_p.T_COD_PROFILO,0,2)  as CAT_USO,
        substr(rcugas_massivo_p.T_COD_PROFILO,4,1)  as CLASSE_PREL,
        TAB_GRADI_GIORNO_ISTAT_PL.ZONA_CLIMATICA as ZONA_CLIM,
        rcugas_massivo_p.T_COD_PROFILO as COD_PROF_STD,                                                                                                     
        rcugas_massivo_p.t_trattamento as TRATTAMENTO,                                                                                                      
        rcugas_connessioni_distr.id_reg_clim as REGIONE_CLIM
    FROM
        rcugas.rcugas_massivo_integ_p as rcugas_massivo_p  
    join
    (
        select 
            max(cast(rcugas_massivo_p.t_anno_termico as int)) as ANNO_RIF, 
            rcugas_massivo_p.t_codice_pdr as PDR
        FROM rcugas.rcugas_massivo_p 
        where rcugas_massivo_p.T_COD_PROFILO IS NOT NULL 
            and rcugas_massivo_p.T_COD_PROFILO <> 'null'
            and rcugas_massivo_p.T_COD_PROFILO <> ''
            and rcugas_massivo_p.n_prelievo_annuo  is not null
            and rcugas_massivo_p.n_prelievo_annuo  <> ''
            --and nvl(rcugas_massivo_p.DATA_FINE_FOR, '') = ''    
        group by rcugas_massivo_p.t_codice_pdr
    ) as t
    join
        SGIGANTE.TAB_GRADI_GIORNO_ISTAT_PL
    join rcugas.rcugas_connessioni_distr3 as rcugas_connessioni_distr
    on TAB_GRADI_GIORNO_ISTAT_PL.CODICE_ATTUALE = rcugas_massivo_p.t_comune_istat_pdr
        and rcugas_massivo_p.t_codice_pdr =  rcugas_connessioni_distr.t_codice_pdr
        and rcugas_massivo_p.t_codice_pdr = t.PDR
        and t.ANNO_RIF=rcugas_massivo_p.t_anno_termico
    
;


-- 17
drop table au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR;
create table au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR stored as PARQUET  as
   select anno_rif,
          pdr,
          (case
               when array_contains(array('C1','C2','C3','C4','C5'), cat_uso) then (
               case
                when cons_annuo < 500 then 'C2'
when cons_annuo >= 500 and cons_annuo <= 5000 then 'C3'
else 'C1'
end                                                             
)
when array_contains(array('T1','T2'), cat_uso) then 'T2'
else null
end
) as cat_uso,
(
case
when array_contains(array('C1','C2','C3','C4','C5'), cat_uso) then 1
when array_contains(array('T1','T2'), cat_uso) then 3
end
) as classe_prel,
(case 
when array_contains(array('C2','C4','T1'), cat_uso) then 'X'
else TAB_RCUGAS_SETTLE_GAS_PDR.zona_clim
end) as zona_clim,
cod_prof_std,
trattamento,
cons_annuo,
regione_clim
from au.TAB_RCUGAS_SETTLE_GAS_PDR
;


------------------------------------------------------------  PARTE 17 BIS 2 --------------------------------------------------------
drop table au_test.fornituredaver;
create table au_test.fornituredaver Stored As PARQUET as
   select rcugas_massivo_semplificata_p.*
   from  rcugas.rcugas_massivo_semplificata_p
   join
   (
        select distinct 
            rcugas_massivo_semplificata_p.t_codice_pdr, 
            rcugas_massivo_semplificata_p.d_data_inizio_for, 
            rcugas_massivo_semplificata_p.DATA_FINE_FOR
        from  rcugas.rcugas_massivo_semplificata_p
        join au.gas_sag
        on rcugas_massivo_semplificata_p.t_codice_pdr=gas_sag.cod_pdr
        where gas_sag.DATA_MIS1 >= rcugas_massivo_semplificata_p.d_data_inizio_for
              and  (NVL(rcugas_massivo_semplificata_p.DATA_FINE_FOR,'') = '' or gas_sag.DATA_MIS1 <= rcugas_massivo_semplificata_p.DATA_FINE_FOR)
    ) as fornituraif
    on  rcugas_massivo_semplificata_p.t_codice_pdr= fornituraif.t_codice_pdr
    where rcugas_massivo_semplificata_p.d_data_inizio_for > fornituraif.DATA_FINE_FOR
          and NVL(fornituraif.DATA_FINE_FOR,'') <> ''
;

drop table au_test.PDRNOCONTFORN;
create table au_test.PDRNOCONTFORN Stored As PARQUET as
    select distinct T.t_codice_pdr
    from (
        select * from au_test.fornituredaver
        where (T_PROCESSO  = 'VTG' or T_PROCESSO  = 'VSG') and T_TIPO_OP  = 'InserisciFORNITURA'
        
        union all 
        
        select fornituredaver.* 
        from au_test.fornituredaver
        join (
            select gas_sag1.* 
            from ( select gas_sag.*, (case when file_name like "/mnt%" then split(file_name,'_')[2] else split(file_name,'_')[1] end) as n_id_pratica from au.gas_sag) as  gas_sag1
            join PRT_RCUGAS.RCUGAS_TEMP_VA1 on RCUGAS_TEMP_VA1.n_id_pratica = gas_sag1.n_id_pratica
            where rcugas_temp_va1.cod_prestazione = 'A01'             
                  and rcugas_temp_va1.cod_prestazione = 'A40'            
                  and rcugas_temp_va1.cod_prestazione = 'VL1'
            ) as tmp on
            tmp.cod_pdr = fornituredaver.t_codice_pdr          
            where ( T_PROCESSO  = 'VARIAZIONE') and T_TIPO_OP  = 'InserisciFORNITURA'
        ) as T
;

drop table au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2;
create table au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2 stored as PARQUET  as
    select * 
    from au.gas_sag
    left join au_test.PDRNOCONTFORN
    on gas_sag.cod_pdr=PDRNOCONTFORN.t_codice_pdr
    where PDRNOCONTFORN.t_codice_pdr is null;

------------------------------------------------------------ FINE PARTE 17 BIS 2 --------------------------------------------------------
-- 18
drop table au.TAB_SETTLE_GAS_PROF_PDR;
create table au.TAB_SETTLE_GAS_PROF_PDR stored as PARQUET  as
select distinct
TAB_RCUGAS_NEW_SETTLE_GAS_PDR.anno_rif,
TAB_RCUGAS_NEW_SETTLE_GAS_PDR.pdr as  pdr,
(case
when gas_tds.cod_pdr IS NOT NULL then gas_tds.cat_uso
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,0,2)
else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cat_uso
end) as cat_uso,
(case 
when gas_tds.cod_pdr IS NOT NULL then gas_tds.classe_prelievo
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,4,1)
else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.classe_prel
end) as classe_prel,
(case
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,3,1) 
else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.zona_clim
end) as zona_clim,
(case
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then gas_sag.cod_prof_prel_std             
else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cod_prof_std
end) as cod_prof_prel_std, 
(case
when gas_sag.cod_pdr IS NOT NULL  and gas_sag.data_mis1 is not null  and gas_sag.data_mis2 is not null then 'M'          
when gas_sag.cod_pdr IS NOT NULL  and gas_sag.data_mis1 is null  and gas_sag.data_mis2 is null then 'G'          
else TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento
end) as trattamento_sag, 
TAB_RCUGAS_NEW_SETTLE_GAS_PDR.trattamento as trattamento,
TAB_RCUGAS_NEW_SETTLE_GAS_PDR.cons_annuo,
TAB_RCUGAS_NEW_SETTLE_GAS_PDR.regione_clim,
(case
when gas_tds.cod_pdr IS NOT NULL then "TDS"
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
else "RCUGAS"
end) as cat_uso_from,
(case
when gas_tds.cod_pdr IS NOT NULL then "TDS"
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
else "RCUGAS"
end) as classe_prel_from,
(case
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
else "RCUGAS"
end) as zona_clim_from,
(case
when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
else "RCUGAS"                                                                                     
end) as cod_prof_prel_std_from,
(case
when gas_sag.cod_pdr IS NOT NULL  and gas_sag.data_mis1 is not null  and gas_sag.data_mis2 is not null then "SAG"          
when gas_sag.cod_pdr IS NOT NULL  and gas_sag.data_mis1 is null  and gas_sag.data_mis2 is null and gas_sag.data_ds is not null then "SAG"          
else "RCUGAS"                                                                                     
end) as trattamento_from
from au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR
left join au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2 on TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2.cod_pdr = TAB_RCUGAS_NEW_SETTLE_GAS_PDR.pdr
left join au.gas_tds on gas_tds.cod_pdr = TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2.cod_pdr
left join au.gas_sag on gas_sag.cod_pdr = TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2.cod_pdr
;

---  Lanciare la correzione del 18

-- 19
drop table au.TAB_PDR_SEGMENTO_PDR_MIS_MM;
create table au.TAB_PDR_SEGMENTO_PDR_MIS_MM stored as PARQUET  as
    select distinct
        t.cod_pdr as PDR,
        t.data_mis1 as DATA_DZ,
        t.data_mis2 as DATA_DZ1,
        datediff(t.data_mis2,t.data_mis1) as diff,
        cast(1.0 as double) as PProfNkDZ,
        cast(1.0 as double) as PProfNkAz,
        ( case
           when t.cod_pdr IS NOT NULL  and t.data_mis1 is not null  and t.data_mis2 is not null then 'M'
           when t.cod_pdr IS NOT NULL  and t.data_mis1 is null  and t.data_mis2 is null then 'G'
           end) as trattamento_sag
        from au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2 as t
        where t.data_mis1 is not null  and t.data_mis2 is not null
;

-- 19
drop table au.TAB_PDR_SEGMENTO_DELTA_MIS_MM;
create table au.TAB_PDR_SEGMENTO_DELTA_MIS_MM stored as PARQUET  as
    select distinct
        t.cod_pdr as PDR,
        t.data_mis1 as DATA_DZ,
        t.data_mis2 as DATA_DZ1,
        datediff(t.data_mis2, t.data_mis1) as diff,
        t.cons_ann as DELTA_MIS,
        t.data_mis1 as DATA_Az,
        t.data_mis2 as DATA_Az1,
        "M" as trattamento_sag,
        rcugas_massivo_max_annotermico_p.t_trattamento as trattamento
        from au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2 as  t
        join rcugas.rcugas_massivo_max_annotermico_p on rcugas_massivo_max_annotermico_p.t_codice_pdr = t.cod_pdr
        where t.data_mis1 is not null  and t.data_mis2 is not null
;

-- 20
drop table au.TAB_CONSUMO_ANNUO_GG;
create table au.TAB_CONSUMO_ANNUO_GG stored as PARQUET  as
      select
        t.cod_pdr as PDR,
        t.cons_ann as CAPdR,
        rcugas_massivo_max_annotermico_p.t_trattamento as trattamento,
        "G" as trattamento_sag,
        cast(NULL as double) as PProfkz,
        cast(NULL as double) as PProfNkDZ,
        cast(NULL as double) as PProfNkAz
    from au.TAB_RCUGAS_NEW_SETTLE_GAS_PDR_17BIS2 as  t
    inner join rcugas.rcugas_massivo_max_annotermico_p on rcugas_massivo_max_annotermico_p.t_codice_pdr = t.cod_pdr
    where t.data_mis1 is null and t.data_mis2 is null
;


drop table au.InputDenominatoreCA;
Create Table au.InputDenominatoreCA Stored As PARQUET as
    SELECT 
        DATA_DZ,
        DATA_DZ1,
        REGIONE_CLIM,
        COD_PROF_STD,
        TAB_RCUGAS_SETTLE_GAS_PDR.pdr
    FROM au.TAB_PDR_SEGMENTO_DELTA_MIS_MM
    join au.TAB_RCUGAS_SETTLE_GAS_PDR
    on TAB_RCUGAS_SETTLE_GAS_PDR.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr
;

-- 21
-- la tabella TAB_SETTLE_GAS_PROF_PDR viene usata per calcolare il denominatore

set hive.enforce.sortmergebucketmapjoin=false;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join= false;

drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE stored as PARQUET  as
SELECT
    TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr, 
    SUM(TAB_PROFILI_GIORN_STD_PERC_FORMAT.pprofk) AS Denominatore
FROM au.TAB_PDR_SEGMENTO_DELTA_MIS_MM
join au.TAB_RCUGAS_SETTLE_GAS_PDR_Bucketed 
join au.TAB_PROFILI_GIORN_STD_PERC_FORMAT
on TAB_PROFILI_GIORN_STD_PERC_FORMAT.id_reg_clim= TAB_RCUGAS_SETTLE_GAS_PDR_Bucketed.REGIONE_CLIM
    and TAB_PROFILI_GIORN_STD_PERC_FORMAT.prof= TAB_RCUGAS_SETTLE_GAS_PDR_Bucketed.COD_PROF_STD
    and TAB_RCUGAS_SETTLE_GAS_PDR_Bucketed.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr 
WHERE TAB_PROFILI_GIORN_STD_PERC_FORMAT.data >= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ
    AND TAB_PROFILI_GIORN_STD_PERC_FORMAT.data <= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1
GROUP BY TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr
;


drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM stored as PARQUET  as
    SELECT DISTINCT                                                                                                                                                                                                          
        TDenominatore.pdr,                                                                                                                                                                                                   
        (DELTA_MIS / TDenominatore.Denominatore) AS CA,                                                                                                                                                                      
        TDenominatore.Denominatore as PProfkz,     
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.trattamento_sag as trattamento_sag,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.trattamento as trattamento
    FROM au.TAB_PDR_SEGMENTO_DELTA_MIS_MM                                                                                                                                                                 
    JOIN au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE as TDenominatore on TDenominatore.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr                                                                                                                                        
    where TDenominatore.Denominatore <> 0 and DELTA_MIS is not NULL
;

-- 22
drop table au.TAB_CONSUMO_ANNUO_MM;
create table au.TAB_CONSUMO_ANNUO_MM  stored as PARQUET  as
    SELECT TAB_PDR_SEGMENTO_CONSUMO_MM.pdr,
        SUM( CA * (CASE WHEN (1/PProfNkDZ) < 1 THEN (1/PProfNkDZ) ELSE 1 END)  * PProfNkAz) as CAPdR, 
        trattamento,
        TAB_PDR_SEGMENTO_CONSUMO_MM.trattamento_sag,
        PProfkz, 
        PProfNkDZ, 
        PProfNkAz
    FROM
        au.TAB_PDR_SEGMENTO_CONSUMO_MM
    join au.TAB_PDR_SEGMENTO_PDR_MIS_MM on TAB_PDR_SEGMENTO_PDR_MIS_MM.pdr = TAB_PDR_SEGMENTO_CONSUMO_MM.pdr
    GROUP BY TAB_PDR_SEGMENTO_CONSUMO_MM.pdr, PProfNkDZ, PProfNkAz, PProfkz, TAB_PDR_SEGMENTO_CONSUMO_MM.trattamento_sag, trattamento
;

drop table au.TAB_CONSUMO_ANNUO;
create table au.TAB_CONSUMO_ANNUO  stored as PARQUET  as
    select
        *, 'TAB_CONSUMO_ANNUO_MM' as from_table
    from au.TAB_CONSUMO_ANNUO_MM
    UNION ALL 
    select
        *, 'TAB_CONSUMO_ANNUO_GG' as from_table
    from au.TAB_CONSUMO_ANNUO_GG
;

-- 23
drop table au.TAB_DATI_SETTLE_SAG;
create table au.TAB_DATI_SETTLE_SAG  stored as PARQUET  as
    select                                                                                             
        TAB_CONSUMO_ANNUO.pdr ,                                                                        
        TAB_CONSUMO_ANNUO.capdr,                                                                    
        TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std,                                                           
        TAB_CONSUMO_ANNUO.trattamento,                                                                       
        TAB_CONSUMO_ANNUO.trattamento_sag,
        TAB_CONSUMO_ANNUO.from_table                                                                    
    from au.TAB_CONSUMO_ANNUO                                        
    join au.TAB_SETTLE_GAS_PROF_PDR on TAB_SETTLE_GAS_PROF_PDR.pdr = TAB_CONSUMO_ANNUO.pdr 
    UNION ALL 
    select                                                                                                   
        TAB_SETTLE_GAS_PROF_PDR.pdr as pdr,                                                                  
        TAB_SETTLE_GAS_PROF_PDR.cons_annuo as capdr,                                                         
        TAB_SETTLE_GAS_PROF_PDR.cod_prof_prel_std,                                                           
        TAB_SETTLE_GAS_PROF_PDR.trattamento,                                                                 
        TAB_SETTLE_GAS_PROF_PDR.trattamento_sag,                                                                 
        'TAB_SETTLE_GAS_PROF_PDR' as from_table                                                              
    from au.TAB_SETTLE_GAS_PROF_PDR                                                       
    left join au.TAB_CONSUMO_ANNUO on TAB_CONSUMO_ANNUO.pdr = TAB_SETTLE_GAS_PROF_PDR.pdr 
    WHERE TAB_CONSUMO_ANNUO.pdr IS NULL        
;


-- Ricalcolo della categoria uso
drop table au.TAB_DATI_SETTLE_SAG_RIC;
create table au.TAB_DATI_SETTLE_SAG_RIC stored as PARQUET  as
    select
      pdr,
      capdr,
      cod_prof_prel_std, 
      substr(cod_prof_prel_std,0,2) as cat_uso, 
      substr(cod_prof_prel_std,4,1) as classe_prel,
      substr(cod_prof_prel_std,3,1) as zona_clim,
      (case
          when capdr < 500 then 'C2'
          when capdr >= 500 and capdr <= 5000 then 'C3'
          else 'C1'
      end) as RES_CAT_USO,
      trattamento,
      trattamento_sag,
      from_table,
      rcugas_connessioni_distr.t_remi,
      rcugas_connessioni_distr.id_reg_clim,
      rcugas_connessioni_distr.n_id_distr,
      '' as cap_trasp_pdr
    from au.TAB_DATI_SETTLE_SAG 
    join rcugas.rcugas_connessioni_distr3  as rcugas_connessioni_distr
        on rcugas_connessioni_distr.t_codice_pdr = TAB_DATI_SETTLE_SAG.pdr
;

-- tabella risultato finale con ricalcolo della cat_uso
drop table au.TAB_DATI_SETTLE_SAG_RES;
create table au.TAB_DATI_SETTLE_SAG_RES stored as PARQUET  as
    select                                                     
      "2020" as anno_competenza,
      rcugas_connessioni_distr3.n_id_distr,
      rcugas_massivo_p.n_id_az_udd,
      rcugas_connessioni_distr3.t_remi as codice_remi,
      TAB_DATI_SETTLE_SAG.pdr as codice_pdr,
      "" as cap_trasp_pdr,
      (case
          when TAB_DATI_SETTLE_SAG.capdr < 500 then 'C2'
          when TAB_DATI_SETTLE_SAG.capdr >= 500 and TAB_DATI_SETTLE_SAG.capdr <= 5000 then 'C3'
          else 'C1'
      end) as cat_uso, 
      substr(TAB_DATI_SETTLE_SAG.cod_prof_prel_std,4,1) as classe_prelievo,
      substr(TAB_DATI_SETTLE_SAG.cod_prof_prel_std,3,1) as zona_climatica,
      rcugas_connessioni_distr3.id_reg_clim,
      TAB_DATI_SETTLE_SAG.cod_prof_prel_std, 
      TAB_DATI_SETTLE_SAG.capdr as prelievo_annuo_prev,
      TAB_DATI_SETTLE_SAG.trattamento,
      cast(current_date  as string) as d_ricezione
      ,trattamento_sag
      from au.TAB_DATI_SETTLE_SAG   
      join rcugas.rcugas_massivo_p  
      join rcugas.rcugas_connessioni_distr3  on 
      rcugas_massivo_p.t_codice_pdr = TAB_DATI_SETTLE_SAG.pdr 
      and rcugas_connessioni_distr3.t_codice_pdr = TAB_DATI_SETTLE_SAG.pdr 
;

-- tabella finale per scrittura in oracle
drop table au.TAB_DATI_SETTLE_SAG_RES_ORACLE;
create table au.TAB_DATI_SETTLE_SAG_RES_ORACLE stored as PARQUET  as
    select            
      row_number() over () as ID_SAG_ANN,                                         
      anno_competenza,
      n_id_distr,
      n_id_az_udd,
      codice_remi,
      codice_pdr,
      cap_trasp_pdr,
      cat_uso, 
      classe_prelievo,
      zona_climatica,
      id_reg_clim,
      cod_prof_prel_std, 
      prelievo_annuo_prev,
      trattamento,
      d_ricezione
    from au.TAB_DATI_SETTLE_SAG_RES
;
