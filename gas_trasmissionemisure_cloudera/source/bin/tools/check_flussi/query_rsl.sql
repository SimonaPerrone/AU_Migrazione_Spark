drop table cmg_gas.report_202003_RSL2;
create table cmg_gas.report_202003_RSL2
STORED AS PARQUET as
with repo as (
    select substr(split(filename,"_")[3],0,3) as flusso, concat(split(path, "/")[3] , split(path, "/")[4] ) as annomesegiorno, filename, path
    from Atg.filescloudgas
    where path like "%RSL%"
),
repo2 as (
    select t_tipo_servizio as flusso, concat(split(t_nome_file, "/")[7], split(t_nome_file, "/")[8]) as annomesegiorno, t_nome_file, t_nome_file as path, max(from_unixtime(unix_timestamp(d_data_caricamento))) as d_caricamento
    from cmg_gas.prt_cmg_file_backeted_p
    where t_nome_file like "%RSL%" 
    group by t_tipo_servizio, t_nome_file
)

select r.tipo, sum(r.c), r.annomesegiorno
from (
    select "PRT" as tipo, count(distinct lower(reverse(split(reverse(t_nome_file),"/")[0])) ) as c, a.annomesegiorno
    from repo2 as a
    where path like "%RSL%"
    group by a.flusso, a.annomesegiorno

    union all

    select "ATG" as tipo, count(distinct split(lower(filename), ".zip")[0]) as c, a.annomesegiorno
    from repo as a
    where path like "%RSL%"
    group by a.flusso, a.annomesegiorno
) as r
group by r.tipo, r.annomesegiorno;

