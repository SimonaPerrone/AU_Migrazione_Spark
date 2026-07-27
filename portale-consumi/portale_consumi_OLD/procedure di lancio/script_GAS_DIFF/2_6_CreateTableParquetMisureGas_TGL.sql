--partizione mese_comp non deriva dal campo di filtro data_comp 
set mapreduce.input.fileinputformat.split.maxsize=134217728;


      
drop table CMG.PRT_CMG_TGL_o;
create table CMG.PRT_CMG_TGL_o Stored As Parquet as
select  cod_pdr,
concat(substr(TGL.mese_comp,3,4),substr(TGL.mese_comp,1,2)) mese_comp ,
date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
tipo_lettura
from CMG_GAS.PRT_CMG_TGL_p TGL
where CAST(date_format(d_caricamento,'yyyyMMdd') as int) >=  ${hiveconf:last_dt_elab_tgl} and tipo_lettura ='E' AND 
cast((date_format(from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')),'yyyyMM')) as int) >=  cast(date_format(date_sub(current_date(),${env:limit_gg_gas}),'yyyyMM') as int) ;






