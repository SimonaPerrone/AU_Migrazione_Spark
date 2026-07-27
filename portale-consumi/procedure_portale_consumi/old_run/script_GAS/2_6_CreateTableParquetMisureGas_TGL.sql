--partizione mese_comp non deriva dal campo di filtro data_comp 
set mapreduce.input.fileinputformat.split.maxsize=134217728;

--create temporary table list_mese_comp stored as parquet as 
--select distinct date_format(Dt,'MMyyyy') mese_comp
-- from
-- (
--  select  t.idrow
--   ,date_add (t.StartDate,pe.i)   as Dt
--  from(select 1 idrow ,current_date()EndDate , date_sub(current_date,396)StartDate)  t
--lateral view 
--posexplode(split(space(datediff(t.EndDate,t.StartDate)),' ')) pe as i,x
--  ) as tbl;
      
drop table CMG.PRT_CMG_TGL_o;
create table CMG.PRT_CMG_TGL_o Stored As Parquet as
select  cod_pdr,
concat(substr(TGL.mese_comp,3,4),substr(TGL.mese_comp,1,2)) mese_comp ,
date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
let_tot_prel,
from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')) as data_comp,
tipo_lettura
from CMG_GAS.PRT_CMG_TGL_p TGL
where tipo_lettura ='E' AND 
cast((date_format(from_unixtime(unix_timestamp(data_comp , 'dd/MM/yyyy')),'yyyyMM')) as int) >=  cast(date_format(date_sub(current_date(),396),'yyyyMM') as int) ;



--utilizzo la partizioni mese_comp che deduco sia l'anno e mese di data_comp 
--where TGL.mese_comp in 
--(
--  select A.mese_comp from list_mese_comp A
--) and tipo_lettura ="E";



