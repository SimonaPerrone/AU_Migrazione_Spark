--- Richiesta da AU di creare delle nuove tabelle 
--- Questo file e' una copia del file Table_HIVE_SAG1_V3.sql

drop table au.REC_20191022_AMM_SAG;
create table au.REC_20191022_AMM_SAG stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_AMM_SAG'
--/user/silvia/au/misure_gas_au
as 
    select 
        valid ,
        LOWER(file_name) as file_name,
        LOWER(file_name_rel) as file_name_rel,
        cod_pdr ,
        cod_remi ,
        cons_ann ,
        cod_prof_prel_std ,
        from_unixtime(unix_timestamp(data_ds , 'dd/MM/yyyy')) as data_ds ,
        from_unixtime(unix_timestamp(data_mis1 , 'dd/MM/yyyy')) as data_mis1 ,
        from_unixtime(unix_timestamp(data_mis2 , 'dd/MM/yyyy')) as data_mis2 ,
        cast(num_riga as int) as num_riga
    from au.AMM_TMP_SAG
;

-- Inesistenti OK`
drop table au.REC_20191022_ruc_sag_inesistenti_ok;
create table au.REC_20191022_ruc_sag_inesistenti_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_inesistenti_ok'
--/user/silvia/au/misure_gas_au
as 
    select distinct tab.* 
    from au.REC_20191022_AMM_SAG as tab
    left join rcugas.rcugas_massivo_p on rcugas_massivo_p.t_codice_pdr = tab.COD_PDR
    where rcugas_massivo_p.t_codice_pdr is not null
;

drop table au.REC_20191022_ruc_sag_inesistenti_ko;
create table au.REC_20191022_ruc_sag_inesistenti_ko stored as PARQUET 
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_inesistenti_ko' 
as 
    select distinct '226' as COD_CAUSALE, "Il PdR e' inesistente" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_AMM_SAG as tab
    left join rcugas.rcugas_massivo_p on rcugas_massivo_p.t_codice_pdr = tab.COD_PDR
    where rcugas_massivo_p.t_codice_pdr is null
;

-- TASK 3
-- CONS_ANN formalmente corretto deve essere numerico, non sono ammessi valori decimali e valori negativi
drop table au.REC_20191022_ruc_sag_task3_ok;
create table au.REC_20191022_ruc_sag_task3_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task3_ok' 
as 
SELECT tab.* from au.REC_20191022_ruc_sag_inesistenti_ok as tab
  where tab.CONS_ANN >= 0
;

drop table au.REC_20191022_ruc_sag_task3_ko;
create table au.REC_20191022_ruc_sag_task3_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task3_ko' 
as 
select distinct '004' as COD_CAUSALE, "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (CONS_ANN non coretto)" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA  
from au.REC_20191022_ruc_sag_inesistenti_ok as tab
  where tab.CONS_ANN < 0
;

-- RCU attivo OK
drop table au.REC_20191022_ruc_sag_attivi_ok;
create table au.REC_20191022_ruc_sag_attivi_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_attivi_ok' 
as 
select distinct tab.* 
from au.REC_20191022_ruc_sag_task3_ok as tab
left join (
select t_codice_pdr,n_id_fornitura, from_unixtime(unix_timestamp(DATA_FINE_FOR , 'yyyy-MM-dd')) as DATA_FINE_FOR
from rcugas.rcugas_massivo_p) as rcugas_massivo_p 
on rcugas_massivo_p.t_codice_pdr = tab.COD_PDR
where rcugas_massivo_p.t_codice_pdr is not null and rcugas_massivo_p.DATA_FINE_FOR is null 
      and rcugas_massivo_p.n_id_fornitura IS NOT NULL
;

drop table au.REC_20191022_ruc_sag_attivi_ko;
create table au.REC_20191022_ruc_sag_attivi_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_attivi_ko' 
as 
select distinct '226' as COD_CAUSALE, "Il PdR non e' attivo" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA  
from au.REC_20191022_ruc_sag_task3_ok as tab
left join au.REC_20191022_ruc_sag_attivi_ok
on REC_20191022_ruc_sag_attivi_ok.COD_PDR=tab.COD_PDR
where REC_20191022_ruc_sag_attivi_ok.COD_PDR is null;

-- TASK 14
drop table au.REC_20191022_ruc_sag_task14_ok;
create table au.REC_20191022_ruc_sag_task14_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task14_ok' 
as  
    select distinct tab.* 
    from au.REC_20191022_ruc_sag_attivi_ok as tab
    WHERE DATA_MIS1 is not null and DATA_MIS2 is not null
        and year(DATA_MIS1) <= split(tab.FILE_NAME_REL, '_')[5] 
        and year(DATA_MIS2) <= split(tab.FILE_NAME_REL, '_')[5] 
    union all
    select distinct tab.* from au.REC_20191022_ruc_sag_attivi_ok as tab
    WHERE DATA_MIS1 is null and DATA_MIS2 is null
;

drop table au.REC_20191022_ruc_sag_task14_ko;
create table au.REC_20191022_ruc_sag_task14_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task14_ko' 
as 
    select distinct '004' as COD_CAUSALE, "I campi obbligatori non sono stati compilati o non son stati correttmente compilati (DATA_MIS1/DATA_MIS2/DATA_DS non devono essere superiori all data trasmissione" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_ruc_sag_attivi_ok as tab
    WHERE DATA_MIS1 is not null and DATA_MIS2 is not null
        and year(DATA_MIS1) > split(tab.FILE_NAME_REL, '_')[5] 
        and year(DATA_MIS2) > split(tab.FILE_NAME_REL, '_')[5] 
;

-- TASK 15
drop table au.REC_20191022_ruc_sag_task15_ok;
create table au.REC_20191022_ruc_sag_task15_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task15_ok' 
as 
    select distinct tab.* 
    from au.REC_20191022_ruc_sag_task14_ok as tab
    WHERE DATA_MIS1 is not null and DATA_MIS2 is not null
        and datediff(data_mis2, data_mis1) >= 270
    union All
    select distinct tab.* 
    from au.REC_20191022_ruc_sag_task14_ok as tab
    WHERE DATA_MIS1 is null and DATA_MIS2 is null
;

drop table au.REC_20191022_ruc_sag_task15_ko;
create table au.REC_20191022_ruc_sag_task15_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task15_ko' 
as 
    select distinct '004' as COD_CAUSALE, "I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (DATA_MIS2-DATA_MIS1 non risulta maggiore uguale a 300 giorni)" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_ruc_sag_task14_ok as tab
    WHERE datediff(data_mis2, data_mis1) < 270
;

-- Task 21
-- PDR nella titolarità del Distributore
drop table au.REC_20191022_ruc_sag_task21_ok;
create table au.REC_20191022_ruc_sag_task21_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task21_ok' 
as 
    with
       dataframe2 as (
            select split(FILE_NAME_REL, "_")[3] as piva, * from au.REC_20191022_ruc_sag_task15_ok
    )

    SELECT distinct tab.valid, tab.file_name, tab.file_name_rel, tab.cod_pdr, tab.cod_remi, tab.cons_ann, tab.cod_prof_prel_std, tab.data_ds, tab.data_mis1, tab.data_mis2, tab.num_riga 
    from dataframe2 as tab
    join rcugas.rcugas_connessioni_distr_p 
    join rcugas.v_rcugas_distributore_p 
    on     v_rcugas_distributore_p.n_id_distributore = rcugas_connessioni_distr_p.n_id_distr
      and tab.COD_PDR = rcugas_connessioni_distr_p.t_codice_pdr 
      and tab.piva = v_rcugas_distributore_p.t_piva
    where nvl(rcugas_connessioni_distr_p.d_data_fine_conn, '') = '' 
;

drop table au.REC_20191022_ruc_sag_task21_ko;
create table au.REC_20191022_ruc_sag_task21_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task21_ko' 
as 
    select distinct '009' as COD_CAUSALE, "PdR non di competenza del distributore" as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_ruc_sag_task15_ok as tab
    left join au.REC_20191022_ruc_sag_task21_ok 
    on tab.COD_PDR = REC_20191022_ruc_sag_task21_ok.COD_PDR
    where REC_20191022_ruc_sag_task21_ok.COD_PDR is null
;

-- TASK 16
-- Filtro per max consumo anno per data_mis2 null; max data_mis2 per data_mis2 not null 
drop table au.REC_20191022_ruc_sag_task16_ok;
create table au.REC_20191022_ruc_sag_task16_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task16_ok' 
as 
    SELECT distinct tab.* from au.REC_20191022_ruc_sag_task21_ok as tab
    join (
        SELECT COD_PDR, 
            null as data_mis2,
            --max(cons_ann) as cons_ann,
            min(num_riga) as num_riga
        from au.REC_20191022_ruc_sag_task21_ok
        where data_mis2 is null
        group by COD_PDR

        union all 

        SELECT COD_PDR, 
            max(data_mis2) as data_mis2, 
            --max(cons_ann) as cons_ann,
            min(num_riga) as num_riga  
        from au.REC_20191022_ruc_sag_task21_ok
        where data_mis2 is not null
        group by COD_PDR
        ) as tab1
    on tab1.COD_PDR = tab.COD_PDR
       --and tab1.num_riga = tab.num_riga
       --and  concat(nvl(cast(tab1.data_mis2 as string),''), cast(tab1.cons_ann as string))  = concat(nvl(cast(tab.data_mis2 as string),''), cast(tab.cons_ann as string))  
       and  concat(nvl(cast(tab1.data_mis2 as string),''), cast(tab1.num_riga as string))  = concat(nvl(cast(tab.data_mis2 as string),''), cast(tab.num_riga as string))  
;

drop table au.REC_20191022_ruc_sag_task16_ko;
create table au.REC_20191022_ruc_sag_task16_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task16_ko' 
as 
    select distinct '004' as COD_CAUSALE, concat("I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" , tab.COD_PDR , " presente piu' volte nel file ", tab.file_name , ")") as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_ruc_sag_task21_ok  as tab
    left join au.REC_20191022_ruc_sag_task16_ok
    on tab.COD_PDR = REC_20191022_ruc_sag_task16_ok.COD_PDR
    where REC_20191022_ruc_sag_task16_ok.COD_PDR is null
;

-- TASK 17
-- Filtro per timestamp presente nel file. 
-- Prelevo il min (per la prima versione 24/07/2019)
drop table au.REC_20191022_ruc_sag_task17_ok;
create table au.REC_20191022_ruc_sag_task17_ok stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task17_ok' 
as 
    with it as (
        select LOWER(file_name_rel) as file_name_rel, cod_pdr, cons_ann from  au.REC_20191022_ruc_sag_task16_ok
        union all
        select regexp_replace(LOWER(file_name),"/mnt/isilonshare1/tisg_sag1/","") as file_name_rel, cod_pdr, cons_ann from  au.gas_sag
    )

    select distinct 
        tab.valid,
        LOWER(tab.file_name) as file_name,
        LOWER(tab.file_name_rel) as file_name_rel,
        tab.cod_pdr,
        tab.cod_remi,
        tab.cons_ann,
        tab.cod_prof_prel_std,
        tab.data_ds,
        tab.data_mis1,
        tab.data_mis2,
        tab.num_riga
    from au.REC_20191022_ruc_sag_task16_ok as tab
    join (  
        select COD_PDR, 
               max(cons_ann) as cons_ann,
               max(cast(split(file_name_rel,'_')[0] as bigint)) as timestamp_check 
        from it
        group by COD_PDR
    ) as tab1
    on       tab1.COD_PDR = tab.COD_PDR 
        and  cast(split(tab.file_name_rel,'_')[0] as bigint) = tab1.timestamp_check
        and  tab1.cons_ann = tab.cons_ann
;

drop table au.REC_20191022_ruc_sag_task17_ko;
create table au.REC_20191022_ruc_sag_task17_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_ruc_sag_task17_ko' 
as 
    select distinct '004' as COD_CAUSALE, concat("I campi obbligatori non sono stati compilati o non sono stati correttamente compilati (" , tab.COD_PDR , " presente piu' volte nel file " , tab.file_name , ")") as DESCRIZIONE, tab.file_name, tab.NUM_RIGA 
    from au.REC_20191022_ruc_sag_task16_ok  as tab
    left join au.REC_20191022_ruc_sag_task17_ok
    on tab.COD_PDR = REC_20191022_ruc_sag_task17_ok.COD_PDR
    where REC_20191022_ruc_sag_task17_ok.COD_PDR is null
;

-- Tabella ANOMALIE
drop table au.REC_20191022_sag_union_ko;
create table au.REC_20191022_sag_union_ko stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_sag_union_ko' 
as 
select * from (
    select cod_causale,  descrizione, file as file_name, num_riga from au.ANOM_TMP
    union all
    select * from au.REC_20191022_ruc_sag_inesistenti_ko
    union all
    select * from au.REC_20191022_ruc_sag_task3_ko
    union all
    select * from au.REC_20191022_ruc_sag_attivi_ko
    union all
    select * from au.REC_20191022_ruc_sag_task14_ko
    union all
    select * from au.REC_20191022_ruc_sag_task15_ko
    union all
    select * from au.REC_20191022_ruc_sag_task16_ko
    union all
    select * from au.REC_20191022_ruc_sag_task17_ko
    union all
    select * from au.REC_20191022_ruc_sag_task21_ko
) as tmp
;

-- Tabella usata per generare i file di ammissibilita
drop table au.REC_20191022_sag_csv;
create table au.REC_20191022_sag_csv stored as PARQUET  
LOCATION '/user/hive/warehouse/settle_gas.db/REC_20191022_sag_csv' 
as 
    select distinct * from     
    ( 
        select valid,          
               FILE_NAME,          
               FILE_NAME_REL,           
               COD_PDR,
               COD_REMI,
               CONS_ANN,
               COD_PROF_PREL_STD,
               DATA_DS,
               DATA_MIS1,
               DATA_MIS2,
               'SAG' as COD_TIPO_FILE,     
               split(file_name_rel,'_')[2] as PIVA_UTENTE,     
               'Y' as VERIFICA_AMM,     
               '' as COD_CAUSALE,     
               '' as DESCRIZIONE,     
               'SAG' as tipo_file,  
               num_riga,          
               UNIX_TIMESTAMP() as data_import          
        from au.REC_20191022_ruc_sag_task17_ok          
    union all          
        select False as valid,          
               FILE_NAME as FILE_NAME,          
               FILE_NAME as FILE_NAME_REL,          
               NULL as COD_PDR,          
               NULL as COD_REMI,          
               NULL as CONS_ANN,          
               NULL as COD_PROF_PREL_STD,
               NULL as DATA_DS,
               NULL as DATA_MIS1,
               NULL as DATA_MIS2,     
               NULL as COD_TIPO_FILE,       
               NULL as PIVA_UTENTE,       
               NULL as VERIFICA_AMM,       
               COD_CAUSALE,       
               DESCRIZIONE,       
               'SAG' as tipo_file,   
               num_riga,          
               UNIX_TIMESTAMP() as data_import          
        from au.REC_20191022_sag_union_ko          
    ) as table2
;


