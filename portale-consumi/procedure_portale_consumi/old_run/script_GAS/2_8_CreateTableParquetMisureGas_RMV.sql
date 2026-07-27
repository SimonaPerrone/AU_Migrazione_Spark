--partizione annomese deriva dal campo di filtro data_comp 
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
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),396),'yyyyMM') as int) ;

