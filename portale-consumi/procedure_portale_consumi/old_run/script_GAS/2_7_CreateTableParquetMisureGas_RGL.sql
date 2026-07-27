--partizione mese_comp non deriva dal campo di filtro data_racc 
set mapreduce.input.fileinputformat.split.maxsize=134217728;

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
where cast((date_format(data_racc,'yyyyMM')) as int) >=  cast(date_format(date_sub(current_date(),396),'yyyyMM') as int) ;
