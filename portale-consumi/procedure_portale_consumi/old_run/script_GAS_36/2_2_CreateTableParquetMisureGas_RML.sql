--partizione annomese deriva dal campo di filtro data_racc 
drop table CMG.PRT_CMG_RML_o; 
Create Table CMG.PRT_CMG_RML_o Stored As Parquet
as
select        cod_pdr,
            annomese_riferimento,
            date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
            let_tot_prel,
            from_unixtime(unix_timestamp(data_racc , 'dd/MM/yyyy')) as data_racc,
            mot_rett_lett as Motivazione    
from
CMG_GAS.PRT_CMG_RML_p
where  cast(annomese as int) >=  cast(date_format(date_sub(current_date(),${env:limit_gg_gas}),'yyyyMM') as int) ;

