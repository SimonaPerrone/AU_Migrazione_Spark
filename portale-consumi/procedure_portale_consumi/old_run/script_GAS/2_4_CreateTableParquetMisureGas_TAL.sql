-- partizione annomese deriva da campo di filtro data_com_autolet_cf 
drop Table     CMG.PRT_CMG_TAL_o;
Create Table CMG.PRT_CMG_TAL_o Stored As Parquet
as 
select
    cod_pdr,
    annomese_riferimento,
    date_format(d_caricamento , 'yyyy-MM-dd HH:mm:ss') as dt_caricamento,
    let_tot_prel,
    from_unixtime(unix_timestamp(data_com_autolet_cf , 'dd/MM/yyyy')) as data_lettura    
from CMG_GAS.PRT_CMG_TAL_p
where cast(annomese as int) >=  cast(date_format(date_sub(current_date(),396),'yyyyMM') as int) ;


