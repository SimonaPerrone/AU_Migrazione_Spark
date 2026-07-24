
create table cmg_gas.report_202003_FLUSSO
STORED AS PARQUET as

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE01/"
from Atg.filescloudgas as a
where path like "%2020/MESE01/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE01/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE01/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE02/"
from Atg.filescloudgas as a
where path like "%2020/MESE02/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE02/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE02/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE03/"
from Atg.filescloudgas as a
where path like "%2020/MESE03/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE03/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE03/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE04/"
from Atg.filescloudgas as a
where path like "%2020/MESE04/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE04/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE04/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE05/"
from Atg.filescloudgas as a
where path like "%2020/MESE05/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE05/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE05/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE06/"
from Atg.filescloudgas as a
where path like "%2020/MESE06/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE06/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE06/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE07/"
from Atg.filescloudgas as a
where path like "%2020/MESE07/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE07/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE07/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE08/"
from Atg.filescloudgas as a
where path like "%2020/MESE08/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE08/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE08/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE09/"
from Atg.filescloudgas as a
where path like "%2020/MESE09/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE09/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE09/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE10/"
from Atg.filescloudgas as a
where path like "%2020/MESE10/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE10/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE10/%" and t_nome_file like "%FLUSSO%"
union all

select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE11/"
from Atg.filescloudgas as a
where path like "%2020/MESE11/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE11/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE11/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE12/"
from Atg.filescloudgas as a
where path like "%2020/MESE12/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) ,"2020/MESE12/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE12/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE13/"
from Atg.filescloudgas as a
where path like "%2020/MESE13/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE13/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE13/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE14/"
from Atg.filescloudgas as a
where path like "%2020/MESE14/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) , "2020/MESE14/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE14/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE15/"
from Atg.filescloudgas as a
where path like "%2020/MESE15/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) ,"2020/MESE15/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE15/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE16/"
from Atg.filescloudgas as a
where path like "%2020/MESE16/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE16/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE16/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]), "2020/MESE17/"
from Atg.filescloudgas as a
where path like "%2020/MESE17/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo,
count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ),"2020/MESE17/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE17/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE18/"
from Atg.filescloudgas as a
where path like "%2020/MESE18/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE18/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE18/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE19/"
from Atg.filescloudgas as a
where path like "%2020/MESE19/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE19/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE19/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE20/"
from Atg.filescloudgas as a
where path like "%2020/MESE20/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE20/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE20/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE21/"
from Atg.filescloudgas as a
where path like "%2020/MESE21/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE21/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE21/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE22/"
from Atg.filescloudgas as a
where path like "%2020/MESE22/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE22/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE22/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE23/"
from Atg.filescloudgas as a
where path like "%2020/MESE23/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE23/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE23/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE24/"
from Atg.filescloudgas as a
where path like "%2020/MESE24/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE24/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE24/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE25/"
from Atg.filescloudgas as a
where path like "%2020/MESE25/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE25/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE25/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE26/"
from Atg.filescloudgas as a
where path like "%2020/MESE26/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE26/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE26/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE27/"
from Atg.filescloudgas as a
where path like "%2020/MESE27/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE27/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE27/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE28/"
from Atg.filescloudgas as a
where path like "%2020/MESE28/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE28/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE28/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE29/"
from Atg.filescloudgas as a
where path like "%2020/MESE29/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE29/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE29/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE30/"
from Atg.filescloudgas as a
where path like "%2020/MESE30/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE30/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE30/%" and t_nome_file like "%FLUSSO%"
union all
select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]),"2020/MESE31/"
from Atg.filescloudgas as a
where path like "%2020/MESE31/%" 
and path like "%FLUSSO%"
union all
select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ), "2020/MESE31/"
from cmg_gas.prt_cmg_file_backeted_p 
where t_nome_file like "%2020/MESE31/%" and t_nome_file like "%FLUSSO%"






