select count(*) 
from (
select 0 as esito, 
       deco.filename_src as Sorgente,
       deco.descrizione as Stato_Decompressione,
       deco.filename_folder_dest as FileDestinazione,
       deco.dataelaborazione
from cmg_gas.report_decompressione as deco
left join (select reverse(split(reverse(t_nome_file),'/')[0]) as t_nome_file  from cmg_gas.prt_cmg_file_backeted_p) as cmg_file
on lower(reverse(split(reverse(deco.filename_folder_dest),'/')[0])) = lower(cmg_file.t_nome_file)
where  cmg_file.t_nome_file is null and deco.filename_folder_dest not like '%IM1%' 
       and cast(deco.dataelaborazione as date) = cast('DATA' as date)  --current_date()
       and lower(deco.filename_src) not like "%.zip"
union all
select 0 as esito, 
       deco.filename_src as Sorgente,
       deco.descrizione as Stato_Decompressione,
       deco.filename_folder_dest as FileDestinazione,
       deco.dataelaborazione
from cmg_gas.report_decompressione as deco
left join (select reverse(split(reverse(t_nome_file),'/')[0]) as t_nome_file  from cmg_gas.prt_cmg_file_im1_p) as cmg_file
on lower(reverse(split(reverse(deco.filename_folder_dest),'/')[0])) = lower(cmg_file.t_nome_file)
where  cmg_file.t_nome_file is null and deco.filename_folder_dest  like '%IM1%'
       and cast(deco.dataelaborazione as date) =  cast('DATA' as date)  --current_date()
       and lower(deco.filename_src) not like "%.zip"
) as c
