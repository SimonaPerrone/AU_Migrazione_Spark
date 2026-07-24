-- VERSIONE 4 
 
/*
    Passo A.1
	-- Creazione tabella parametri caratteristici
*/

drop table au.TAB_PARAMETRI_CARATTERISTICI_PROFILO;
create table au.TAB_PARAMETRI_CARATTERISTICI_PROFILO stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PARAMETRI_CARATTERISTICI_PROFILO' 
as
select *,
    concat(zona_clim, cast(class_prelievo as int)) as KeyZonaClasse
from au.TAB_PARAMETRI_CARATTERISTICI_PROF_PREL
;

/*
    Passo A.2
*/
drop table au.ParametriPProf;
create table au.ParametriPProf stored as PARQUET   
LOCATION '/user/hive/warehouse/settle_gas.db/ParametriPProf' 
as
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
/*
    Passo A.3
*/
drop table au.TAB_PROFILI_GIORN_STD_PERC;
create table au.TAB_PROFILI_GIORN_STD_PERC stored as PARQUET   
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PROFILI_GIORN_STD_PERC' 
as
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


/*
    Passo A.4
	-- Creazione del nuovo formato data.
*/
drop table au.TAB_PROFILI_GIORN_STD_PERC_FORMAT;
create table au.TAB_PROFILI_GIORN_STD_PERC_FORMAT stored as PARQUET    
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PROFILI_GIORN_STD_PERC_FORMAT' 
as
    select 
        from_unixtime(unix_timestamp(data , 'dd/MM/yyyy')) as data,
        pprofk,
        id_reg_clim,
        prof,
        wkr
    from au.TAB_PROFILI_GIORN_STD_PERC
;


/*
    Passo B.1
	-- Sono presenti tutti i campi della pdr relativo dell'ultimo anno termico
*/
drop table rcugas.rcugas_massivo_max_annotermico_p;
create table rcugas.rcugas_massivo_max_annotermico_p stored as PARQUET      
LOCATION '/user/hive/warehouse/settle_gas.db/rcugas_massivo_max_annotermico_p' 
as
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

/*
    Passo B.2
	-- per calcolare la REGIONE_CLIM devo prelevare dalla tabella RCUGAS.REMI_ANAGRAFICA (Connessioni_Distr3)
*/
drop table au.TAB_RCUGAS_SETTLE_GAS_PDR_V2_INTER;
create table au.TAB_RCUGAS_SETTLE_GAS_PDR_V2_INTER stored as PARQUET        
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_GAS_PDR_V2_INTER' 
as
    select DISTINCT
        cast(rcugas_massivo_p.n_prelievo_annuo as double) as CONS_ANNUO,
        t.ANNO_RIF, 
        rcugas_massivo_p.t_codice_pdr as PDR,
        (
            case when nvl(rcugas_massivo_p.T_COD_PROFILO,'') = '' and nvl(rcugas_massivo_p.t_cod_cat_uso, '') = ''  then
               (
                   case
                        when rcugas_massivo_p.n_prelievo_annuo < 500 then 'C2'
                        when rcugas_massivo_p.n_prelievo_annuo >= 500 and rcugas_massivo_p.n_prelievo_annuo <= 5000 then 'C3'
                        else 'C1'
                    end   
               ) 
            when nvl(rcugas_massivo_p.T_COD_PROFILO,'') <> '' and nvl(rcugas_massivo_p.t_cod_cat_uso, '') = '' then 
                substr(rcugas_massivo_p.T_COD_PROFILO,0,2)
            else rcugas_massivo_p.t_cod_cat_uso
            end
        ) as CAT_USO,
        (
            case when nvl(rcugas_massivo_p.T_COD_PROFILO,'') = '' and nvl(rcugas_massivo_p.t_cod_classe_prelievo, '') = ''  then
               (
                   case 
                        when array_contains(array('C1','C2','C3','C4','C5'), rcugas_massivo_p.t_cod_cat_uso) then '1'
                        when array_contains(array('T1','T3'), rcugas_massivo_p.t_cod_cat_uso) then '3'
                        else '1'
                    end
               ) 
            when nvl(rcugas_massivo_p.T_COD_PROFILO,'') <> '' and nvl(rcugas_massivo_p.t_cod_classe_prelievo, '') = '' then 
                substr(rcugas_massivo_p.T_COD_PROFILO,4,1)
            else rcugas_massivo_p.t_cod_classe_prelievo
            end
        ) as CLASSE_PREL,
        rcugas_connessioni_distr.id_reg_clim as REGIONE_CLIM,
        TAB_GRADI_GIORNO_ISTAT_PL.ZONA_CLIMATICA as ZONA_CLIM,
        rcugas_massivo_p.T_COD_PROFILO,
        rcugas_massivo_p.t_trattamento as TRATTAMENTO                                                                                                      
    FROM
        rcugas.rcugas_massivo_integ_p as rcugas_massivo_p  
    join
    (
        select 
            max(cast(rcugas_massivo_p.t_anno_termico as int)) as ANNO_RIF, 
            rcugas_massivo_p.t_codice_pdr as PDR
        FROM rcugas.rcugas_massivo_p 
        where 
            nvl(rcugas_massivo_p.n_prelievo_annuo, '') <> ''
            and nvl(rcugas_massivo_p.DATA_FINE_FOR,'') = ''
            and nvl(rcugas_massivo_p.n_id_fornitura, '' ) <> ''
        group by rcugas_massivo_p.t_codice_pdr
    ) as t
    join
        SGIGANTE.TAB_GRADI_GIORNO_ISTAT_PL
    join rcugas.rcugas_connessioni_distr3 as rcugas_connessioni_distr
    on TAB_GRADI_GIORNO_ISTAT_PL.CODICE_ATTUALE = rcugas_massivo_p.t_comune_istat_pdr
        and rcugas_massivo_p.t_codice_pdr =  rcugas_connessioni_distr.t_codice_pdr
        and rcugas_massivo_p.t_codice_pdr = t.PDR
        and t.ANNO_RIF=rcugas_massivo_p.t_anno_termico
    
    --where PDR = '02490000018537'
;


/*
    Passo B.3
	-- tabella contenente tutti i PDR attivi in RCUGAS con le informazioni delle regione e zona climatica
*/

drop table au.TAB_RCUGAS_SETTLE_GAS_PDR_V2;
create table au.TAB_RCUGAS_SETTLE_GAS_PDR_V2 stored as PARQUET        
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_GAS_PDR_V2' 
as
    select 
        CONS_ANNUO,
        ANNO_RIF, 
        PDR,
        CAT_USO,
        CLASSE_PREL,
        REGIONE_CLIM,
        ZONA_CLIM,
        TRATTAMENTO, 
        (
            case 
                when nvl(T_COD_PROFILO,'') = '' then 
                concat(CAT_USO, ZONA_CLIM, CLASSE_PREL )
                else T_COD_PROFILO
            end
        ) as COD_PROF_STD
    from  au.TAB_RCUGAS_SETTLE_GAS_PDR_V2_INTER
;           
------------------------------------------------------------  SAG Continuita fornitura --------------------------------------------------------
-- COntiene tutte le prestazioni che sono intervenute sui PDR del SAG

/*
drop table au.fornituredaver;
create table au.fornituredaver Stored As PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/fornituredaver' 
as 
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
    --    where (gas_sag.DATA_MIS1 >= rcugas_massivo_semplificata_p.d_data_inizio_for
    --          and  (NVL(rcugas_massivo_semplificata_p.DATA_FINE_FOR,'') = '' or gas_sag.DATA_MIS1 <= rcugas_massivo_semplificata_p.DATA_FINE_FOR) )
	--			or
	--			(rcugas_massivo_semplificata_p.d_data_inizio_for > gas_sag.DATA_MIS1)
			where   
				(rcugas_massivo_semplificata_p.d_data_inizio_for < gas_sag.DATA_MIS1  and (NVL(rcugas_massivo_semplificata_p.DATA_FINE_FOR,'') = '' or gas_sag.DATA_MIS1 <= rcugas_massivo_semplificata_p.DATA_FINE_FOR))
				or
				(rcugas_massivo_semplificata_p.d_data_inizio_for > gas_sag.DATA_MIS1)
    ) as fornituraif
    on  rcugas_massivo_semplificata_p.t_codice_pdr= fornituraif.t_codice_pdr
    where rcugas_massivo_semplificata_p.d_data_inizio_for > fornituraif.DATA_FINE_FOR
          and NVL(fornituraif.DATA_FINE_FOR,'') <> ''
;
--*/
/*
    Passo C.1
	-- verfica tutte le forniture che presentano una data inizio successiva alla data_mis1 proveniente dal SAG1.
*/

drop table au.fornituredaver;
create table au.fornituredaver Stored As PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/fornituredaver' 
as 
   select rcugas_massivo_semplificata_p.*
        from  rcugas.rcugas_massivo_semplificata_p
        join au.gas_sag
        on rcugas_massivo_semplificata_p.t_codice_pdr=gas_sag.cod_pdr 
           where   (rcugas_massivo_semplificata_p.d_data_inizio_for > gas_sag.DATA_MIS1);
     
;



/*
    Passo C.2
*/
/*
drop table au.PDRNOCONTFORN;
create table au.PDRNOCONTFORN Stored As PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/PDRNOCONTFORN' 
as 
    select distinct T.t_codice_pdr
    from (
        select * from au.fornituredaver
        where (T_PROCESSO  = 'VTG' or T_PROCESSO  = 'VSG') and T_TIPO_OP  = 'InserisciFORNITURA'
        
        union all 
        
        select fornituredaver.* 
        from au.fornituredaver
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
*/

-- Tabella che presenta tutti i PDR provenienti dal SAG che non presentano una continuità di fornitura tra la data_mis1 alla data di calcolo. Tale condizione è soddisfatta se la fornitura ha INSERISCI_FORNITURA E COD_PROCESSO IN (VTG, VSG, VARIAZIONE)
drop table au.PDRNOCONTFORN;
create table au.PDRNOCONTFORN Stored As PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/PDRNOCONTFORN' 
as 
    select distinct T.t_codice_pdr
    from (
        select * from au.fornituredaver
        where (T_PROCESSO  = 'VTG' or T_PROCESSO  = 'VSG') and T_TIPO_OP  = 'InserisciFORNITURA'
        
        union all 
        
        select fornituredaver.* 
        from au.fornituredaver
        join (
            select cod_pdr, n_id_pratica 
            from rcugas.rcugas_massivo_semplificata_p --(rcugas_massivo prendere N_ID_PRATICA_PROCESSO)
            join PRT_RCUGAS.RCUGAS_TEMP_VA1 on RCUGAS_TEMP_VA1.n_id_pratica = rcugas_massivo_semplificata_p.N_ID_PRATICA_PROCESSO 
            where rcugas_temp_va1.cod_prestazione in ('A01', 'A40', 'VL1')
            ) as tmp on
            tmp.cod_pdr = fornituredaver.t_codice_pdr          
            where ( T_PROCESSO  = 'VARIAZIONE') and T_TIPO_OP  = 'InserisciFORNITURA'
        ) as T
;



/*
    Passo C.3
	--Tabella contente tutti i punti provenienti dal SAG1 e che presentano una continuità di fornitura SAG1_OK
*/


drop table au.TAB_SAG1_CONT_FORN;
create table au.TAB_SAG1_CONT_FORN stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_CONT_FORN' 
as 
    select gas_sag.* 
    from au.gas_sag
    left join au.PDRNOCONTFORN
    on gas_sag.cod_pdr=PDRNOCONTFORN.t_codice_pdr
    where PDRNOCONTFORN.t_codice_pdr is null  
         and ( (DATA_MIS1 > '2016-1-1' and data_mis2 > '2016-1-1') or DATA_MIS1 is null);
			
-- Inserire filtro per eliminare elementi minori del 1/1/2016

------------------------------------------------------------ FINE PARTE 17 BIS 2 - Continuità di Fornitura dai dati SAG1 --------------------------------------------------------

/*
    Passo D.1
    Identifica i pdr su cui ci sono solo i CA Dedotti, ovvero su cui non è arrivato un SAG valido (SAG_OK)
*/
drop table au.TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR;
create table au.TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR' 
as
    select TAB_RCUGAS_SETTLE_GAS_PDR_V2.* 
    from au.TAB_RCUGAS_SETTLE_GAS_PDR_V2
    left join au.TAB_SAG1_CONT_FORN
    on TAB_SAG1_CONT_FORN.cod_pdr = TAB_RCUGAS_SETTLE_GAS_PDR_V2.PDR
    where TAB_SAG1_CONT_FORN.cod_pdr is null
;

/* Passo D.2
    Ricalcola la categoria d'uso, classe di prelievo, zona_clim secondo i passaggi successivi:
        1- verifica la presenza di TDS o SAG validi e li usa per i nuovi valori
        2- Regole di calcolo a partire dal CA
 */
drop table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_INTER;
create table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_INTER stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_CODPROFSTDNEW_INTER' 
as 
    select distinct
        TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.anno_rif,
        TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.pdr as  pdr,
        (case 
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.cat_uso
            when gas_prestazioni01072019_2.t_codice_pdr IS NOT NULL then gas_prestazioni01072019_2.t_cod_cat_uso
            when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,0,2)
            --else 
            --( case
            --    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.CONS_ANNUO < 500 then 'C2'
            --    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.CONS_ANNUO >= 500 and TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.CONS_ANNUO <= 5000 then 'C3'
            --    else 'C1'
            -- end
            when array_contains(array('C1','C2','C3','C4','C5'), TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cat_uso) then (
               case
                    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo < 500 then 'C2'
                    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo >= 500 and TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo <= 5000 then 'C3'
                    else 'C1'
               end                                                             
               )
            when array_contains(array('T1','T2'), TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cat_uso) then 'T2'
            else 
               (case
                    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo < 500 then 'C2'
                    when TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo >= 500 and TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo <= 5000 then 'C3'
                    else 'C1'
               end)
          end
        ) as CAT_USO,
        (case 
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.classe_prelievo
            when gas_prestazioni01072019_2.t_codice_pdr IS NOT NULL then gas_prestazioni01072019_2.t_cod_classe_prelievo
            when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,4,1)
            else TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.CLASSE_PREL
--            (     -- Non si ricalcola la classe di prelievo
               --  case 
                --        when nvl(TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO,'') <> '' and  array_contains(array('T1','T2'), TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO) then 3
                --        else '1'
                -- end
  --          )                        
        end) as classe_prel,
        (case
            when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(gas_sag.cod_prof_prel_std,3,1) 
            else TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.zona_clim
        end) as zona_clim,
        COD_PROF_STD as t_cod_profilo,
        TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.trattamento as trattamento,
        TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.cons_annuo,
        TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.regione_clim,
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
        end) as cod_prof_prel_std_from
    from au.TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR
    left join au.gas_tds on gas_tds.cod_pdr = TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.pdr
    left join au.gas_sag on gas_sag.cod_pdr = TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.pdr
    left join atg.gas_prestazioni01072019_2 on gas_prestazioni01072019_2.t_codice_pdr = TAB_RCUGAS_SETTLE_GAS_DEDOTTI_PDR.pdr
;

/*  Passo D.3
    Ricalcola il codice di profilo standard come concatenazione dei valori 
      categoria d'uso, classe di prelievo, zona_clim
*/
drop table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW;
create table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_CODPROFSTDNEW' 
as 
    SELECT 
        anno_rif,
        pdr,
        CAT_USO,
        classe_prel,
        zona_clim, 
        concat(CAT_USO, zona_clim, classe_prel) as cod_prof_prel_std,
        trattamento,
        cons_annuo,
        regione_clim,
        cat_uso_from,
        classe_prel_from,
        zona_clim_from,
        cod_prof_prel_std_from
    from au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_INTER;

    
/*  Passo D.4
    Ricalcola il codice di profilo standard e la zona climatica per inserire 
    la zona climatica X
*/    
drop table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_X;
create table au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_X stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_RCUGAS_SETTLE_CODPROFSTDNEW_X' 
as
    select 
        anno_rif,
        pdr,
        cat_uso,
        classe_prel,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then 
                    'X'
                else zona_clim
            end
        ) as zona_clim,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then 
                    concat(cat_uso, 'X', classe_prel)
                else cod_prof_prel_std
            end
        ) as  cod_prof_prel_std,
        trattamento,
        cons_annuo,
        regione_clim,
        cat_uso_from,
        classe_prel_from,
        zona_clim_from,
        cod_prof_prel_std_from
    
    from au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW
;


------------------------------------------------------ FINE PARTE RCUGAS_MASSIVO - SAG CONT FORN --------------------------------------------------


/*  Passo E.1
    Identifica i pdr su cui abbiamo SAG validi da utilizzare per il calcolo della CA
*/
drop table au.TAB_SAG1_SETTLE_GAS_PDR;
create table au.TAB_SAG1_SETTLE_GAS_PDR stored as PARQUET        
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_GAS_PDR' 
as
    select  
        TAB_SAG1_CONT_FORN.valid              ,
        TAB_SAG1_CONT_FORN.file_name          ,
        TAB_SAG1_CONT_FORN.giorno_riferimento ,
        TAB_SAG1_CONT_FORN.cod_pdr            ,
        TAB_SAG1_CONT_FORN.cod_remi           ,
        TAB_SAG1_CONT_FORN.cons_ann           ,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cod_prof_std ,
        TAB_SAG1_CONT_FORN.cod_prof_prel_std as  cod_prof_prel_std_sag ,
        TAB_SAG1_CONT_FORN.data_ds            ,
        TAB_SAG1_CONT_FORN.data_mis1          ,
        TAB_SAG1_CONT_FORN.data_mis2          ,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.ZONA_CLIM,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.regione_clim

    from au.TAB_RCUGAS_SETTLE_GAS_PDR_V2
    join au.TAB_SAG1_CONT_FORN
    ON TAB_SAG1_CONT_FORN.cod_pdr = TAB_RCUGAS_SETTLE_GAS_PDR_V2.pdr
;


/*  Passo E.2
    Identifica i pdr di tipo MM  a partire da quelli con SAG calcolati al punto precedente (E.1) -- I pdr di tipo MM sono quelli tale per cui sono state valorizzate DATA_MIS1 e DATA_MIS2   
    */
drop table au.TAB_PDR_SEGMENTO_PDR_MIS_MM;
create table au.TAB_PDR_SEGMENTO_PDR_MIS_MM stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_PDR_MIS_MM' 
as 
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
           end) as trattamento_sag,
        ZONA_CLIM,
        regione_clim
        from au.TAB_SAG1_SETTLE_GAS_PDR as t
        where t.data_mis1 is not null  and t.data_mis2 is not null
;



/*  Passo E.3
    Prepara i dati per il calcolo del CA recuperando il trattamento dal massivo rispetto ai dati del punto precedente. Per questi il campo cons_ann sarà considerato come il delta al numeratore.
         
    */
drop table au.TAB_PDR_SEGMENTO_DELTA_MIS_MM;
create table au.TAB_PDR_SEGMENTO_DELTA_MIS_MM stored as PARQUET     
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_DELTA_MIS_MM' 
as 
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
        from au.TAB_SAG1_SETTLE_GAS_PDR as  t
        join rcugas.rcugas_massivo_max_annotermico_p on rcugas_massivo_max_annotermico_p.t_codice_pdr = t.cod_pdr
        where t.data_mis1 is not null  and t.data_mis2 is not null        
;

/*  Passo E.4
    Identifica i pdr di tipo GG  a partire da quelli con SAG calcolati al punto E1. Anche per questi dal massivo verrà recuperato il campo trattamento mentre il campo Cons_ann sarà considerato come consumo annuo calcolato
    */
drop table au.TAB_CONSUMO_ANNUO_GG;
create table au.TAB_CONSUMO_ANNUO_GG stored as PARQUET   
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_CONSUMO_ANNUO_GG' 
as 
    select 
        t.cod_pdr as PDR,
        max(cast(t.cons_ann as double) ) as CAPdR,
        rcugas_massivo_max_annotermico_p.t_trattamento as trattamento,
        "G" as trattamento_sag,
        cast(NULL as double) as PProfkz,
        cast(NULL as double) as PProfNkDZ,
        cast(NULL as double) as PProfNkAz,
        t.regione_clim,
        t.zona_clim
    from au.TAB_SAG1_SETTLE_GAS_PDR as  t
    inner join rcugas.rcugas_massivo_max_annotermico_p on rcugas_massivo_max_annotermico_p.t_codice_pdr = t.cod_pdr
    where t.data_mis1 is null and t.data_mis2 is null
    group by t.cod_pdr, rcugas_massivo_max_annotermico_p.t_trattamento, t.regione_clim, t.zona_clim
;

/*  Passo E.5
   Create una tabella ottimizzata per il calcolo parallelo  
    -------------------------------------------------------
    */
drop table au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed;
CREATE TABLE `au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed`(
  `cons_annuo` double,
  `anno_rif` int,
  `pdr` string,
  `cat_uso` string,
  `classe_prel` string,
  `zona_clim` string,
  `cod_prof_std` string,
  `trattamento` string,
  `regione_clim` string)
CLUSTERED BY (pdr)
SORTED BY (pdr ASC)
INTO 32 BUCKETS
stored as PARQUET   
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_GAS_PDR_Bucketed' 
;

/*  Passo E.6
   Inserisce i dati del massivo nella tabella dei dati ottimizzata di SAG1 MM al punto precedente E.5
    -------------------------------------------------------
    */
INSERT OVERWRITE TABLE au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed 
    select 
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cons_annuo
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.anno_rif
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.pdr
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.cat_uso
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.classe_prel
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.zona_clim
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.cod_prof_std
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.trattamento
        ,TAB_RCUGAS_SETTLE_GAS_PDR_V2.regione_clim    
    from au.TAB_RCUGAS_SETTLE_GAS_PDR_V2
    join au.TAB_SAG1_CONT_FORN
    on TAB_SAG1_CONT_FORN.cod_pdr = TAB_RCUGAS_SETTLE_GAS_PDR_V2.PDR
;


-- la tabella TAB_SAG1_SETTLE_GAS_PDR_Bucketed viene usata per calcolare il denominatore 
set hive.enforce.sortmergebucketmapjoin=false;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join= false;


/*  Passo E.7
   Calcola il valore del denominatore nella formula per il calcolo della CA per i punti MM  per il singolo segmento
    -------------------------------------------------------
    */
drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE stored as PARQUET 
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE' 
as 
SELECT
    TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr, 
    SUM(TAB_PROFILI_GIORN_STD_PERC_FORMAT.pprofk) AS Denominatore
FROM au.TAB_PDR_SEGMENTO_DELTA_MIS_MM
join au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed 
join au.TAB_PROFILI_GIORN_STD_PERC_FORMAT
on TAB_PROFILI_GIORN_STD_PERC_FORMAT.id_reg_clim= TAB_SAG1_SETTLE_GAS_PDR_Bucketed.REGIONE_CLIM
    and TAB_PROFILI_GIORN_STD_PERC_FORMAT.prof= TAB_SAG1_SETTLE_GAS_PDR_Bucketed.COD_PROF_STD
    and TAB_SAG1_SETTLE_GAS_PDR_Bucketed.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr 
WHERE TAB_PROFILI_GIORN_STD_PERC_FORMAT.data >= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ
    AND TAB_PROFILI_GIORN_STD_PERC_FORMAT.data <= TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1
GROUP BY TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr
;


/*  Passo E.8
   Calcolo della CA del singolo Segmento per i punti MM, ovvero divide Deltamis per il denominatore del punto E.7 e lo moltiplica per 100.
    -------------------------------------------------------
    */
drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_CONSUMO_MM' 
as 
    SELECT DISTINCT
        TDenominatore.pdr,
        (DELTA_MIS / (TDenominatore.Denominatore )) * 100 as CA,
        TDenominatore.Denominatore as PProfkz,     
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.DATA_DZ1,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.trattamento_sag as trattamento_sag,
        TAB_PDR_SEGMENTO_DELTA_MIS_MM.trattamento as trattamento
    FROM au.TAB_PDR_SEGMENTO_DELTA_MIS_MM                                                                                                                                                                 
    JOIN au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE as TDenominatore on TDenominatore.pdr = TAB_PDR_SEGMENTO_DELTA_MIS_MM.pdr                                                                                                                                        
    where TDenominatore.Denominatore <> 0 and DELTA_MIS is not NULL
;


/* Passo E.9 
	Somma i CA calcolati per tutti i Segmenti dei PDR MM , Memorizzando anche il campo Pprofkz 
*/
drop table au.TAB_CONSUMO_ANNUO_MM;
create table au.TAB_CONSUMO_ANNUO_MM  stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_CONSUMO_ANNUO_MM' 
as 
    SELECT TAB_PDR_SEGMENTO_CONSUMO_MM.pdr,
        SUM( CA * (CASE WHEN (1/PProfNkDZ) < 1 THEN (1/PProfNkDZ) ELSE 1 END)  * PProfNkAz)  as CAPdR, 
        trattamento,
        TAB_PDR_SEGMENTO_CONSUMO_MM.trattamento_sag,
        PProfkz, 
        PProfNkDZ, 
        PProfNkAz,
        TAB_PDR_SEGMENTO_PDR_MIS_MM.zona_clim,
        regione_clim
    FROM
        au.TAB_PDR_SEGMENTO_CONSUMO_MM
    join au.TAB_PDR_SEGMENTO_PDR_MIS_MM on TAB_PDR_SEGMENTO_PDR_MIS_MM.pdr = TAB_PDR_SEGMENTO_CONSUMO_MM.pdr
    GROUP BY TAB_PDR_SEGMENTO_CONSUMO_MM.pdr, PProfNkDZ, PProfNkAz, PProfkz, TAB_PDR_SEGMENTO_CONSUMO_MM.trattamento_sag, trattamento, zona_clim, regione_clim
;


/* 
Passo E.10
Tabella di relazione del CA calcolato con il massivo per recuperare le info relative al cod_prof_Std
*/
drop table au.TAB_CONSUMO_ANNUO_MM_PDR;
create table au.TAB_CONSUMO_ANNUO_MM_PDR  stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_CONSUMO_ANNUO_MM_PDR' 
as 
    select
        TAB_CONSUMO_ANNUO_MM.pdr,
        TAB_CONSUMO_ANNUO_MM.CAPdR,
        TAB_CONSUMO_ANNUO_MM.trattamento,
        TAB_CONSUMO_ANNUO_MM.trattamento_sag,
        TAB_CONSUMO_ANNUO_MM.PProfkz, 
        TAB_CONSUMO_ANNUO_MM.PProfNkDZ, 
        TAB_CONSUMO_ANNUO_MM.PProfNkAz,
        TAB_CONSUMO_ANNUO_MM.zona_clim,
        TAB_CONSUMO_ANNUO_MM.regione_clim,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.ANNO_RIF,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cat_uso,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.classe_prel,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cod_prof_std
    from au.TAB_CONSUMO_ANNUO_MM
    join au.TAB_RCUGAS_SETTLE_GAS_PDR_V2
    on TAB_CONSUMO_ANNUO_MM.pdr = TAB_RCUGAS_SETTLE_GAS_PDR_V2.pdr
;



/*
	Passo E.11
	Ricalcola la categoria d'uso, la classe di prelievo,zona climatica secondo le regole in funzione della nuova CA calcolata 
*/
drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM' 
as 
    select distinct
        TAB_CONSUMO_ANNUO_MM_PDR.pdr as  pdr,
        (case
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.cat_uso
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,0,2)
            when array_contains(array('C1','C2','C3','C4','C5'), TAB_CONSUMO_ANNUO_MM_PDR.cat_uso) then (
               case
                    when TAB_CONSUMO_ANNUO_MM_PDR.CAPdR < 500 then 'C2'
                    when TAB_CONSUMO_ANNUO_MM_PDR.CAPdR >= 500 and TAB_CONSUMO_ANNUO_MM_PDR.CAPdR <= 5000 then 'C3'
                    else 'C1'
               end                                                             
               )
            when array_contains(array('T1','T2'), TAB_CONSUMO_ANNUO_MM_PDR.cat_uso) then 'T2'
            else 
               (case
                    when TAB_CONSUMO_ANNUO_MM_PDR.CAPdR < 500 then 'C2'
                    when TAB_CONSUMO_ANNUO_MM_PDR.CAPdR >= 500 and TAB_CONSUMO_ANNUO_MM_PDR.CAPdR <= 5000 then 'C3'
                    else 'C1'
               end)
          end
        ) as CAT_USO,
        (case 
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.classe_prelievo
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,4,1)
            else TAB_CONSUMO_ANNUO_MM_PDR.classe_prel
--            (     -- Non si ricalcola la classe di prelievo
               --  case 
                --        when nvl(TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO,'') <> '' and  array_contains(array('T1','T2'), TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO) then 3
                --        else '1'
                -- end
  --          )            
        end) as classe_prel,
        (case
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,3,1) 
            else TAB_CONSUMO_ANNUO_MM_PDR.zona_clim
        end) as zona_clim,
        COD_PROF_STD as t_cod_profilo,
        TAB_CONSUMO_ANNUO_MM_PDR.trattamento as trattamento,
        TAB_CONSUMO_ANNUO_MM_PDR.trattamento_sag as trattamento_sag,
        TAB_CONSUMO_ANNUO_MM_PDR.CAPdR,
        TAB_CONSUMO_ANNUO_MM_PDR.regione_clim,
        (case
            when gas_tds.cod_pdr IS NOT NULL then "TDS"
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
            else "---"
        end) as cat_uso_from,
        (case
            when gas_tds.cod_pdr IS NOT NULL then "TDS"
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
            else "---"
        end) as classe_prel_from,
        (case
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
            else "---"
        end) as zona_clim_from
        -- (case
        --     when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
        --     else "RCUGAS"                                                                                     
        -- end) as cod_prof_prel_std_from
    from au.TAB_CONSUMO_ANNUO_MM_PDR
    left join au.gas_tds on gas_tds.cod_pdr = TAB_CONSUMO_ANNUO_MM_PDR.pdr
    left join au.TAB_SAG1_CONT_FORN on TAB_SAG1_CONT_FORN.cod_pdr = TAB_CONSUMO_ANNUO_MM_PDR.pdr
;

/*
		Passo E.12
		Costruisce il nuovo Codice di Prelievo Standard a partire dai 3 cambi di base calcolati al punto precedente 
*/
drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW' 
as 
    select 
    
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.pdr                ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.cat_uso            ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.classe_prel        ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.zona_clim          ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.trattamento        ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.trattamento_sag    ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.capdr              ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.regione_clim       ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.cat_uso_from       ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.classe_prel_from   ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.zona_clim_from     ,

        concat(TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.cat_uso, TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.zona_clim, TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.classe_prel) as cod_prof_prel_std

    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM
    left join au.TAB_SAG1_CONT_FORN on TAB_SAG1_CONT_FORN.cod_pdr = TAB_SAG1_SETTLE_CODPROFSTDNEW_INTERM.pdr
;



/*
		Passo E.13
		Aggiorna la zona climativa a X nei casi C2,C4,T1
*/
drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_X;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_X stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_X' 
as
    select 
        pdr,
        cat_uso,
        classe_prel,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then 
                    concat(cat_uso, 'X', classe_prel)
                else cod_prof_prel_std
            end
        ) as  cod_prof_prel_std,
        trattamento,
        trattamento_sag,
        capdr,
        regione_clim,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then  'X'
                else zona_clim
            end
        ) as  zona_clim,
        cat_uso_from,
        classe_prel_from,
        zona_clim_from
        --cod_prof_prel_std_from
    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW
;


/* 
		Passo E.14
		Effettua il controllo della coerenza sul Cod_Prof_Std nuovo rispetto al CA calcolato al punto E.9: in caso positivo non viene effettuato il nuovo calcolo del CA
*/
drop table au.TAB_SAG1_SETTLE_COERZ_SI;
create table au.TAB_SAG1_SETTLE_COERZ_SI stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_COERZ_SI' 
as 
    select *
    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_X
    where 
        ( case
                when TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR < 500 then 'C2'
                when TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR >= 500 and TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR <= 5000 then 'C3'
                else 'C1'
             end
        ) = cat_uso
;
/* 
		Passo E.15
		Effettua il controllo della coerenza sul Cod_Prof_Std nuovo rispetto al CA calcolato al punto E.9: in caso negativo viene effettuato il nuovo calcolo del CA
*/
drop table au.TAB_SAG1_SETTLE_COERZ_NO;
create table au.TAB_SAG1_SETTLE_COERZ_NO stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_COERZ_NO' 
as 
    select 
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.pdr                     ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.cat_uso                 ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.classe_prel             ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.cod_prof_prel_std       ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.trattamento             ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.trattamento_sag         ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.capdr                   ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.regione_clim            ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.zona_clim               ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.cat_uso_from            ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.classe_prel_from        ,
        TAB_SAG1_SETTLE_CODPROFSTDNEW_X.zona_clim_from          ,
       -- TAB_SAG1_SETTLE_CODPROFSTDNEW_X.cod_prof_prel_std_from  ,
        TAB_SAG1_CONT_FORN.data_ds                              ,
        TAB_SAG1_CONT_FORN.data_mis1                            ,
        TAB_SAG1_CONT_FORN.data_mis2                            ,
        TAB_SAG1_CONT_FORN.cons_ann as DELTA_MIS

    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_X
    join au.TAB_SAG1_CONT_FORN
    on TAB_SAG1_SETTLE_CODPROFSTDNEW_X.pdr = TAB_SAG1_CONT_FORN.cod_pdr
    where 
        ( case
                when TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR < 500 then 'C2'
                when TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR >= 500 and TAB_SAG1_SETTLE_CODPROFSTDNEW_X.CAPdR <= 5000 then 'C3'
                else 'C1'
             end
        ) <> cat_uso
;


/*
	Passo E.16 
	 Create una tabella ottimizzata per il calcolo ricalcolato parallelo 
*/
drop table au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO;
CREATE TABLE `au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO`(
  `CAPdR` double,
  `pdr` string,
  `cat_uso` string,
  `classe_prel` string,
  `zona_clim` string,
  `cod_prof_prel_std` string,
  `trattamento` string,
  `regione_clim` string)
CLUSTERED BY (pdr)
SORTED BY (pdr ASC)
INTO 32 BUCKETS
stored as PARQUET   
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO' 
;

/*
	Passo E.17
	Inserisce i dati del massivo nella tabella dei dati ottimizzata di SAG1 MM al punto precedente E.16
*/
INSERT OVERWRITE TABLE au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO 
    select 
        capdr        ,
        pdr          ,
        cat_uso      ,
        classe_prel  ,
        zona_clim    ,
        cod_prof_prel_std ,
        trattamento  ,
        regione_clim 
    from au.TAB_SAG1_SETTLE_COERZ_NO
;



-- la tabella TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO viene usata per calcolare il denominatore 

set hive.enforce.sortmergebucketmapjoin=false;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join= false;

/*  Passo E.18
   Calcola il valore del denominatore  RICALCOLATO nella formula per il calcolo della CA per i punti MM per il singolo segmento
    -------------------------------------------------------
    */

drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE_RICALCOLATO;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE_RICALCOLATO stored as PARQUET 
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE_RICALCOLATO' 
as 
SELECT
    TAB_SAG1_SETTLE_COERZ_NO.pdr, 
    SUM(TAB_PROFILI_GIORN_STD_PERC_FORMAT.pprofk) AS Denominatore
FROM au.TAB_SAG1_SETTLE_COERZ_NO
join au.TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO 
join au.TAB_PROFILI_GIORN_STD_PERC_FORMAT
on TAB_PROFILI_GIORN_STD_PERC_FORMAT.id_reg_clim= TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO.REGIONE_CLIM
    and TAB_PROFILI_GIORN_STD_PERC_FORMAT.prof= TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO.cod_prof_prel_std
    and TAB_SAG1_SETTLE_GAS_PDR_Bucketed_RICALCOLATO.pdr = TAB_SAG1_SETTLE_COERZ_NO.pdr 
WHERE TAB_PROFILI_GIORN_STD_PERC_FORMAT.data >= TAB_SAG1_SETTLE_COERZ_NO.data_mis1
    AND TAB_PROFILI_GIORN_STD_PERC_FORMAT.data <= TAB_SAG1_SETTLE_COERZ_NO.data_mis2
GROUP BY TAB_SAG1_SETTLE_COERZ_NO.pdr
;

	/*  
	Passo E.19
	Calcolo della CA RICALCOLATO del singolo Segmento per i punti MM, ovvero divide Deltamis per il denominatore del punto E.18 e lo moltiplica per 100.
    -------------------------------------------------------
    */
drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO' 
as 
    SELECT DISTINCT
        TDenominatore.pdr,
        (DELTA_MIS / (TDenominatore.Denominatore )) * 100 as CA,
        TDenominatore.Denominatore as PProfkz,     
        TAB_SAG1_SETTLE_COERZ_NO.data_mis1,
        TAB_SAG1_SETTLE_COERZ_NO.data_mis2,
        TAB_SAG1_SETTLE_COERZ_NO.trattamento as trattamento
    FROM au.TAB_SAG1_SETTLE_COERZ_NO                                                                                                                                                                 
    JOIN au.TAB_PDR_SEGMENTO_CONSUMO_MM_DENOMINATORE_RICALCOLATO as TDenominatore on TDenominatore.pdr = TAB_SAG1_SETTLE_COERZ_NO.pdr                                                                                                                                        
    where TDenominatore.Denominatore <> 0 and DELTA_MIS is not NULL
;

/*
	Passo E.20
	Per tutti i punti il cui CA è stato ricalcolato si vanno a recuperare le informazioni relative al coid_prof_Std nuovo
	
*/

drop table au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO_FINALE;
create table au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO_FINALE stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO_FINALE' 
as 
select 
    TAB_SAG1_SETTLE_COERZ_NO.pdr                     ,
    TAB_SAG1_SETTLE_COERZ_NO.cat_uso                 ,
    TAB_SAG1_SETTLE_COERZ_NO.classe_prel             ,
    TAB_SAG1_SETTLE_COERZ_NO.cod_prof_prel_std       ,
    TAB_SAG1_SETTLE_COERZ_NO.trattamento             ,
    TAB_SAG1_SETTLE_COERZ_NO.trattamento_sag         ,
    TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO.ca       ,
    TAB_SAG1_SETTLE_COERZ_NO.regione_clim            ,
    TAB_SAG1_SETTLE_COERZ_NO.zona_clim               ,
    TAB_SAG1_SETTLE_COERZ_NO.cat_uso_from            ,
    TAB_SAG1_SETTLE_COERZ_NO.classe_prel_from        ,
    TAB_SAG1_SETTLE_COERZ_NO.zona_clim_from          
    --TAB_SAG1_SETTLE_COERZ_NO.cod_prof_prel_std_from  
from au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO
join au.TAB_SAG1_SETTLE_COERZ_NO
on TAB_SAG1_SETTLE_COERZ_NO.pdr = TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO.pdr
;


/*
Passo E.21
Tabella dei CA calcolati direttamente dai flussi SAG validi con le informazioni provenienti dal rcugas calcolate al punto B.3

*/

drop table au.TAB_CONSUMO_ANNUO_GG_PDR;
create table au.TAB_CONSUMO_ANNUO_GG_PDR  stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_CONSUMO_ANNUO_GG_PDR' 
as 
    select
        TAB_CONSUMO_ANNUO_GG.pdr,
        TAB_CONSUMO_ANNUO_GG.CAPdR,
        TAB_CONSUMO_ANNUO_GG.trattamento,
        TAB_CONSUMO_ANNUO_GG.trattamento_sag,
        TAB_CONSUMO_ANNUO_GG.PProfkz, 
        TAB_CONSUMO_ANNUO_GG.PProfNkDZ, 
        TAB_CONSUMO_ANNUO_GG.PProfNkAz,
        TAB_CONSUMO_ANNUO_GG.zona_clim,
        TAB_CONSUMO_ANNUO_GG.regione_clim,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.ANNO_RIF,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cat_uso,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.classe_prel,
        TAB_RCUGAS_SETTLE_GAS_PDR_V2.cod_prof_std
    from au.TAB_CONSUMO_ANNUO_GG
    join au.TAB_RCUGAS_SETTLE_GAS_PDR_V2
    on TAB_CONSUMO_ANNUO_GG.pdr = TAB_RCUGAS_SETTLE_GAS_PDR_V2.pdr
;


	/*
		Passo E.22
		Calcolo del Codice Prof Std per i punti GG provenienti dal SAG secondo le regole di prevalenza
	*/

drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER' 
as 
    select distinct
        TAB_CONSUMO_ANNUO_GG_PDR.pdr as  pdr,
        (case
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.cat_uso
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,0,2)
            when array_contains(array('C1','C2','C3','C4','C5'), TAB_CONSUMO_ANNUO_GG_PDR.cat_uso) then (
               case
                    when TAB_CONSUMO_ANNUO_GG_PDR.CAPdR < 500 then 'C2'
                    when TAB_CONSUMO_ANNUO_GG_PDR.CAPdR >= 500 and TAB_CONSUMO_ANNUO_GG_PDR.CAPdR <= 5000 then 'C3'
                    else 'C1'
               end                                                             
               )
            when array_contains(array('T1','T2'), TAB_CONSUMO_ANNUO_GG_PDR.cat_uso) then 'T2'
            else 
               (case
                    when TAB_CONSUMO_ANNUO_GG_PDR.CAPdR < 500 then 'C2'
                    when TAB_CONSUMO_ANNUO_GG_PDR.CAPdR >= 500 and TAB_CONSUMO_ANNUO_GG_PDR.CAPdR <= 5000 then 'C3'
                    else 'C1'
               end)
        end) as CAT_USO,
        (case 
            when gas_tds.cod_pdr IS NOT NULL then gas_tds.classe_prelievo
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,4,1)
            else TAB_CONSUMO_ANNUO_GG_PDR.classe_prel
--            (   --Non si ricalcola la classe di prelievo
               --  case 
                --        when nvl(TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO,'') <> '' and  array_contains(array('T1','T2'), TAB_CONSUMO_ANNUO_GG_PDR.CAT_USO) then 3
                --        else '1'
                -- end
  --          )
        end) as classe_prel,
        (case
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null  then substr(TAB_SAG1_CONT_FORN.cod_prof_prel_std,3,1) 
            else TAB_CONSUMO_ANNUO_GG_PDR.zona_clim
        end) as zona_clim,
        -- (case
        --     when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null  then gas_sag.cod_prof_prel_std             
        --     else (
        --         case when nvl(TAB_PDR_SEGMENTO_CONSUMO_MM.cod_prof_std,'')<>'' then TAB_PDR_SEGMENTO_CONSUMO_MM.cod_prof_std
        --             else concat(TAB_PDR_SEGMENTO_CONSUMO_MM.cat_uso, TAB_PDR_SEGMENTO_CONSUMO_MM.zona_clim, TAB_PDR_SEGMENTO_CONSUMO_MM.classe_prel)
        --         end
        --     )
        -- end) as cod_prof_prel_std,
        TAB_CONSUMO_ANNUO_GG_PDR.trattamento as trattamento,
        TAB_CONSUMO_ANNUO_GG_PDR.trattamento_sag as trattamento_sag,
        TAB_CONSUMO_ANNUO_GG_PDR.CAPdR,
        TAB_CONSUMO_ANNUO_GG_PDR.regione_clim,
        (case
            when gas_tds.cod_pdr IS NOT NULL then "TDS"
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
            else "---"
        end) as cat_uso_from,
        (case
            when gas_tds.cod_pdr IS NOT NULL then "TDS"
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"
            else "---"
        end) as classe_prel_from,
        (case
            when TAB_SAG1_CONT_FORN.cod_pdr IS NOT NULL and REGEXP_REPLACE(TAB_SAG1_CONT_FORN.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
            else "---"
        end) as zona_clim_from ,
        -- (case
        --     when gas_sag.cod_pdr IS NOT NULL and REGEXP_REPLACE(gas_sag.cod_prof_prel_std, '\\p{Print}*', null) is not null then "SAG"                                                      
        --     else "RCUGAS"                                                                                     
        -- end) as cod_prof_prel_std_from
        TAB_CONSUMO_ANNUO_GG_PDR.ANNO_RIF as ANNO_RIF_old,
        TAB_CONSUMO_ANNUO_GG_PDR.cat_uso as cat_uso_old,
        TAB_CONSUMO_ANNUO_GG_PDR.classe_prel as classe_prel_old,
        TAB_CONSUMO_ANNUO_GG_PDR.cod_prof_std as cod_prof_std_old
    from au.TAB_CONSUMO_ANNUO_GG_PDR
    left join au.gas_tds on gas_tds.cod_pdr = TAB_CONSUMO_ANNUO_GG_PDR.pdr
    left join au.TAB_SAG1_CONT_FORN on TAB_SAG1_CONT_FORN.cod_pdr = TAB_CONSUMO_ANNUO_GG_PDR.pdr

;

	/* 
		Passo E.23
		Concatenazione del COD Prof Std per i punti GG provenienti dal SAG1
		
	*/
drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_GG' 
as 
    select 
    
        pdr              ,
        cat_uso          ,
        classe_prel      ,
        zona_clim        ,
        trattamento      ,
        trattamento_sag  ,
        capdr            ,
        regione_clim     ,
        cat_uso_from     ,
        zona_clim_from   ,
        concat(TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER.cat_uso, TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER.zona_clim, TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER.classe_prel) as cod_prof_prel_std , 
        ANNO_RIF_old,
        cat_uso_old,
        classe_prel_old,
        cod_prof_std_old
    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_INTER
;



/*
	Passo E.24
	Ricalcolo del Cod_Prof_StD assegnando la X alla zona climatica nel caso in cui la CAT_USO IN (C1,C2,T1)
*/
drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_X;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_X stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_X' 
as
    select 
        pdr,
        cat_uso,
        classe_prel,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then 
                    concat(cat_uso, 'X', classe_prel)
                else cod_prof_prel_std
            end
        ) as  cod_prof_prel_std,
        trattamento,
        trattamento_sag,
        capdr,
        regione_clim,
        (
            case
                when array_contains(array('C2','C4','T1'), cat_uso) then  'X'
                else zona_clim
            end
        ) as  zona_clim,
               
        anno_rif_old,
        cat_uso_old,
        classe_prel_old,
        cod_prof_std_old,        
        cat_uso_from,
        zona_clim_from
        
        --cod_prof_prel_std_from
    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG
;


------------------------------------------------------------------CREAZIONE DELLA TABELLA UNION DEI MM GG E DEDOTTI.---------------------------------------------------------------------------------------

/*

Passo F.1
-- Calcolo della tabella unione dei punti MM GG e Dedotti in RCUGAS)
*/

drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE' 
as 
-- Massivo
select 
    pdr                    ,
    cat_uso                ,
    classe_prel            ,
    zona_clim              ,
    cod_prof_prel_std      ,
    (
        case when trattamento = 'X' then ''
        else trattamento
        end
    ) as trattamento            ,
    '' as trattamento_sag            ,
    cons_annuo             ,
    regione_clim           
from au.TAB_RCUGAS_SETTLE_CODPROFSTDNEW_X

union all
-- GG
select 
    pdr                  ,
    cat_uso              ,
    classe_prel          ,
    zona_clim            ,
    cod_prof_prel_std    ,
    trattamento          ,
    trattamento_sag            ,
    capdr  as cons_annuo ,
    regione_clim         
-- from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG  -- INSERIMENTO GESTIONE X
from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GG_X

union ALL
-- MM NO
select 
    pdr                   ,
    cat_uso               ,
    classe_prel           ,
    zona_clim             ,
    cod_prof_prel_std     ,
    trattamento           ,
    trattamento_sag            ,
    ca    as cons_annuo   ,
    regione_clim          
from  au.TAB_PDR_SEGMENTO_CONSUMO_MM_RICALCOLATO_FINALE

union ALL

-- MM SI
select 
    pdr                    ,
    cat_uso                ,
    classe_prel            ,
    zona_clim              ,
    cod_prof_prel_std      ,
    trattamento            ,
    trattamento_sag            ,
    capdr    as cons_annuo ,
    regione_clim           
from au.TAB_SAG1_SETTLE_COERZ_SI
;

/*
	PASSO F.2 
	Trattamento sbianchettato nel caso in cui ci sia il valore X.

*/

drop table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW;
create table au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW stored as PARQUET       
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW' 
as 
select 
    pdr                    ,
    cat_uso                ,
    classe_prel            ,
    zona_clim              ,
    cod_prof_prel_std      ,
    CASE WHEN trattamento = 'X' THEN NULL ELSE  trattamento END trattamento,
    trattamento_sag            ,
    capdr    as cons_annuo ,
    regione_clim
from  au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE
;	

/*
	PASSO F.3
	Creazione della tabella preposta al caricamento sotto Oracle schema TISG.PRT_SAG_SETTLEMENT_ANNUALE
*/

drop table au.TAB_DATI_SETTLE_SAG_RES_ORACLE;
create table au.TAB_DATI_SETTLE_SAG_RES_ORACLE stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/TAB_DATI_SETTLE_SAG_RES_ORACLE' 
as
    select 
        21695901 + row_number() over () as ID_SAG_ANN,
        '2020' as anno_competenza, 
        rcugas_connessioni_distr3.n_id_distr,
        rcugas_massivo_p.n_id_az_udd,
        rcugas_connessioni_distr3.t_remi as codice_remi,
        pdr as codice_pdr,
        "" as cap_trasp_pdr,
        cat_uso ,
        classe_prel as classe_prelievo,
        zona_clim   as zona_climatica,
        regione_clim as id_reg_clim,
        cod_prof_prel_std,  
        cast(cons_annuo as int) as prelievo_annuo_prev,
        trattamento,  
        cast(current_date  as string) as d_ricezione
    from au.TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW
    join rcugas.rcugas_massivo_p  
    join rcugas.rcugas_connessioni_distr3  on 
        rcugas_massivo_p.t_codice_pdr = TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW.pdr 
        and rcugas_connessioni_distr3.t_codice_pdr = TAB_SAG1_SETTLE_CODPROFSTDNEW_GENERALE_NEW.pdr
    where nvl(rcugas_massivo_p.data_fine_for,'') = '' and nvl(rcugas_massivo_p.n_id_fornitura,'') <> ''
        and nvl(rcugas_connessioni_distr3.d_data_fine_conn,'') = ''
;
