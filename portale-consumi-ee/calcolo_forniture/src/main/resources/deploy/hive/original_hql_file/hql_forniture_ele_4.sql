create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

drop table if exists mongodbs.fasce;

create TABLE mongodbs.fasce stored as parquet as     
SELECT n_id_pod,n_id_misuratore,MAX(F_LUNEDI)F_LUNEDI,MAX(F_MARTEDI)F_MARTEDI,MAX(F_MERCOLEDI)F_MERCOLEDI,
MAX(F_GIOVEDI)F_GIOVEDI,MAX(F_VENERDI)F_VENERDI,MAX(F_SABATO)F_SABATO,MAX(F_DOMENICA)F_DOMENICA,MAX(F_FESTIVO)F_FESTIVO,
MAX(d_inizio_validita)d_inizio_validita,MAX(d_fine_validita)d_fine_validita,
MAX(d_fine_validita_str)d_fine_validita_str,MAX(d_data_iniziofreezing)d_data_iniziofreezing
FROM 
(
SELECT n_id_pod,n_id_misuratore,
case F_LUNEDI when '1' then fasce else '' end F_LUNEDI ,
case F_MARTEDI when '1' then fasce else '' end F_MARTEDI,
case F_MERCOLEDI when '1' then fasce else '' end F_MERCOLEDI,
case F_GIOVEDI when '1' then fasce else '' end F_GIOVEDI,
case F_VENERDI when '1' then fasce else '' end F_VENERDI,
case F_SABATO when '1' then fasce else '' end F_SABATO,
case F_DOMENICA when '1' then fasce else '' end F_DOMENICA,
case F_FESTIVO when '1' then fasce else '' end F_FESTIVO,
CAST(CASE WHEN nvl(d_inizio_validita,'')='' THEN concat( year(date_sub(current_date,1126)),lpad(month(date_sub(current_date,1126)),2,0),'01') ELSE CAST(CONCAT(SUBSTR(d_inizio_validita,1,4),SUBSTR(d_inizio_validita,6,2),SUBSTR(d_inizio_validita,9,2)) AS INT) END AS BIGINT)d_inizio_validita,
CAST(CASE WHEN nvl(d_fine_validita,'')='' THEN concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) ELSE CAST(CONCAT(SUBSTR(d_fine_validita,1,4),SUBSTR(d_fine_validita,6,2),SUBSTR(d_fine_validita,9,2)) AS INT) END AS BIGINT) d_fine_validita ,
CASE WHEN nvl(d_fine_validita,'')='' THEN '' ELSE CONCAT(SUBSTR(d_fine_validita,1,4),SUBSTR(d_fine_validita,6,2),SUBSTR(d_fine_validita,9,2)) END  d_fine_validita_str ,
CASE WHEN nvl(d_data_iniziofreezing,'')='' THEN d_data_iniziofreezing ELSE CONCAT(SUBSTR(d_data_iniziofreezing,1,4),SUBSTR(d_data_iniziofreezing,6,2),SUBSTR(d_data_iniziofreezing,9,2))  END  d_data_iniziofreezing 
FROM(
SELECT
    misuratore.n_id_pod,n_id_misuratore,
    case n_cod_giorno_2g  when '1' then '1' else '0' end F_LUNEDI ,
    case n_cod_giorno_2g  when '2' then '1' else '0' end F_MARTEDI ,
    case n_cod_giorno_2g  when '3' then '1' else '0' end F_MERCOLEDI ,
    case n_cod_giorno_2g  when '4' then '1' else '0' end F_GIOVEDI ,
    case n_cod_giorno_2g  when '5' then '1' else '0' end F_VENERDI ,
    case n_cod_giorno_2g  when '6' then '1' else '0' end F_SABATO ,
    case n_cod_giorno_2g  when '7' then '1' else '0' end F_DOMENICA ,
    case n_cod_giorno_2g  when '8' then '1' else '0' end F_FESTIVO ,
    CONCAT(
    Concat(nvl(fasce2.n_fine_fascia_1,''),Concat(nvl(concat('-',fasce2.n_fascia_1),''))),',',
    Concat(nvl(concat(fasce2.n_fine_fascia_2),''),Concat(nvl(concat('-',fasce2.n_fascia_2),''))),',',
    Concat(nvl(concat(fasce2.n_fine_fascia_3),''),Concat(nvl(concat('-',fasce2.n_fascia_3),''))),',',
    Concat(nvl(concat(fasce2.n_fine_fascia_4),''),Concat(nvl(concat('-',fasce2.n_fascia_4),''))),',',
    Concat(nvl(concat(fasce2.n_fine_fascia_5),''),Concat(nvl(concat('-',fasce2.n_fascia_5),''))),',',
    Concat(nvl(concat(fasce2.n_fine_fascia_6),''),Concat(nvl(concat('-',fasce2.n_fascia_6),'')))
    ) fasce ,
    d_inizio_validita,d_fine_validita,d_data_iniziofreezing
FROM (select case n_fascia_1 when '' then null else n_fascia_1 end n_fascia_1  ,
             case n_fascia_2 when '' then null else n_fascia_2 end n_fascia_2  ,
             case n_fascia_3 when '' then null else n_fascia_3 end n_fascia_3  ,
             case n_fascia_4 when '' then null else n_fascia_4 end n_fascia_4  ,
             case n_fascia_5 when '' then null else n_fascia_5 end n_fascia_5  ,
             case n_fascia_6 when '' then null else n_fascia_6 end n_fascia_6  ,
             case n_fine_fascia_1 when '' then null else n_fine_fascia_1 end n_fine_fascia_1  ,
             case n_fine_fascia_2 when '' then null else n_fine_fascia_2 end n_fine_fascia_2  ,
             case n_fine_fascia_3 when '' then null else n_fine_fascia_3 end n_fine_fascia_3  ,
             case n_fine_fascia_4 when '' then null else n_fine_fascia_4 end n_fine_fascia_4  ,
             case n_fine_fascia_5 when '' then null else n_fine_fascia_5 end n_fine_fascia_5  ,
             case n_fine_fascia_6 when '' then null else n_fine_fascia_6 end n_fine_fascia_6  ,
             n_cod_giorno_2g,n_id_misuratore,d_aggiornamento ,max(d_aggiornamento) over ( partition by n_id_misuratore,n_cod_giorno_2g) max_aggiornamento
      from RCU.RCU_FASCE_MISURATORE_2G_p  where isNumeric(n_id_misuratore)) fasce2
LEFT OUTER join (select n_id_pod,d_inizio_validita,d_fine_validita,d_data_iniziofreezing,n_id_misuratore_2g from
RCU.RCU_MISURATORE_2G_p  where isNumeric(n_id_misuratore_2g) and isNumeric(n_id_pod) )misuratore on SUBSTR(misuratore.n_id_misuratore_2g,1,18) = SUBSTR(fasce2.n_id_misuratore,1,18)
where  d_aggiornamento = max_aggiornamento and n_id_pod is not null 
) AS TTX
) AS FFX
GROUP BY n_id_pod,n_id_misuratore;

