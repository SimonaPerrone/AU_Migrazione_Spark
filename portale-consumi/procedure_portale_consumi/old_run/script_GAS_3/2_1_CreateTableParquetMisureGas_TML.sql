--partizione annomese deriva dal campo di filtro data_racc
drop TABLE CMG.PRT_CMG_TML_o;
Create Table CMG.PRT_CMG_TML_o  Stored As Parquet
as
select      cod_pdr,
            annomese_riferimento,
            date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
            tipo_lettura            
from
CMG_GAS.PRT_CMG_TML_P
where tipo_lettura ="E"      
and   cast(annomese as int) >=  cast(date_format(date_sub(current_date(),125),'yyyyMM') as int) ;



    
  

