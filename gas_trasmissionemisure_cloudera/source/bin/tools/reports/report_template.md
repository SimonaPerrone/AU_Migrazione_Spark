# REPORT del DATA riferito al giorno ***PERIODO***

``` sql

select count(distinct split(lower(filename),'.zip')[0]) from cmg_gas.report_file	
where filename_src like '%PERIODO%'
    and dataelaborazione='DATA'

select count(*) from  report_file	
where filename_src like "%PERIODO%"
    and dataelaborazione="DATA"
    and filename like "%FLUSSO%"
```

***Numero totale dei file individuati con la funzione RECUPERO_TXT distinti = NUMBER_DISTINCT_TXT***

***Numero di file individuati di tipo:*** 
- RGL NUMBER_RGL
- RML NUMBER_RML
- RSL NUMBER_RSL
- RMV NUMBER_RMV
- SW1 NUMBER_SW1
- TAL NUMBER_TAL
- TAS NUMBER_TAS
- TAV NUMBER_TAV
- TGL NUMBER_TGL
- TML NUMBER_TML
- TMV NUMBER_TMV
- DEF NUMBER_DEF
- FUI NUMBER_FUI
- IM1 NUMBER_IM1


--------
``` sql
select count(*) from (
    select split(lower(filename), ".zip")[0] as filename from  report_file	
    where filename_src like "%PERIODO%"
    and dataelaborazione="DATA"
) as r
left join report_decompressione as p
on r.filename = split(reverse(split(reverse(lower(p.filename_src)),"/")[0]), ".zip")[0]
where p.filename_src is null 
```
***Numero di file non presenti nella tabella REPORT_DECOMPRESSIONE = NUMBER_REPORT_DECOMPRESSI***

--------
## Numero di file xml e zip in REPORT_DECOMPRESSIONE

``` sql
select count(distinct filename_src) from cmg_gas.report_decompressione
where filename_src like'%PERIODO%'
```
Numero: NUMBER_REPORT_2_DECOMPRESSI



# Zeppelin 

## Lista dei file non presenti in REPORT_DECOMPRESSIONE

``` sql
select cout(r.filename) from (
    select split(lower(filename), ".zip")[0] as filename from  cmg_gas.report_file	
    where filename_src like "%PERIODO%"
    and dataelaborazione="DATA"
) as r
left join cmg_gas.report_decompressione as p
on r.filename = split(reverse(split(reverse(lower(p.filename_src)),"/")[0]), ".zip")[0]
where p.filename_src is null 
```
Risultato: NUMBER_ZEPPELIN_1_REPORT_DECOMPRESSI

--------
## Numero KO

``` sql

select count(*) 
from (
select 0 as esito, 
       deco.filename_src as Sorgente,
       deco.descrizione as Stato_Decompressione,
       deco.filename_folder_dest as FileDestinazione,
       deco.dataelaborazione
from cmg_gas.report_decompressione as deco
left join (select reverse(split(reverse(t_nome_file),"/")[0]) as t_nome_file  from cmg_gas.prt_cmg_file_backeted_p) as cmg_file
on lower(reverse(split(reverse(deco.filename_folder_dest),"/")[0])) = lower(cmg_file.t_nome_file)
where  cmg_file.t_nome_file is null and deco.filename_folder_dest not like "%IM1%" 
       and cast(deco.dataelaborazione as date) = cast('DATA' as date)  --current_date()
       and lower(deco.filename_src) not like "%.zip"
union all
select 0 as esito, 
       deco.filename_src as Sorgente,
       deco.descrizione as Stato_Decompressione,
       deco.filename_folder_dest as FileDestinazione,
       deco.dataelaborazione
from cmg_gas.report_decompressione as deco
left join (select reverse(split(reverse(t_nome_file),"/")[0]) as t_nome_file  from cmg_gas.prt_cmg_file_im1_p) as cmg_file
on lower(reverse(split(reverse(deco.filename_folder_dest),"/")[0])) = lower(cmg_file.t_nome_file)
where  cmg_file.t_nome_file is null and deco.filename_folder_dest  like "%IM1%"
       and cast(deco.dataelaborazione as date) =  cast('DATA' as date)  --current_date()
       and lower(deco.filename_src) not like "%.zip"
) as c
```

Numero KO: NUMBER_ZEPPELIN_REPORT_2


--------
## Lista FILE duplicati

``` sql
select sum(c) as tot 
from (
       select filename_folder_dest, count(filename_folder_dest) as c
       from report_decompressione
       where cast(dataelaborazione as date) = "DATA"
             and lower(deco.filename_src) not like "%.zip"
       group by filename_folder_dest
       having c > 1
) as T
```

Numero Totale dei file duplicat: NUMBER_ZEPPELIN_3_REPORT

