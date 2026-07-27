create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

drop table mongodbs.forniture;
drop table mongodbs.forniture_info;

create TEMPORARY  TABLE forniture_tmp STORED AS PARQUET AS
SELECT DISTINCT n_id_fornitura,
CASE WHEN inizio < d_inizio_start THEN d_inizio_start ELSE inizio end inizio,fine ,d_inizio_str,d_fine_str,codice_pod,attivo,n_id_pod,n_id_fornitore,t_tipo_mercato,n_id_cliente,n_id_indirizzo,n_id_ind_forn
 FROM (
select  n_id_fornitura,inizio,fine ,codice_pod,attivo,
n_id_pod,n_id_fornitore,n_id_cliente,t_tipo_mercato,nvl(n_id_indirizzo,'')n_id_indirizzo,nvl(n_id_ind_forn,'')n_id_ind_forn,
min(inizio) over ( partition by codice_pod,fine)min_inizio,d_inizio_str,d_fine_str,d_inizio_start
from (
select n_id_fornitura,
CAST(CASE WHEN d_inizio='' THEN concat( year(date_sub(current_date,${env:limit_gg})),lpad(month(date_sub(current_date,${env:limit_gg})),2,0),'01') ELSE CAST(CONCAT(SUBSTR(d_inizio,1,4),SUBSTR(d_inizio,6,2),SUBSTR(d_inizio,9,2)) AS INT) END AS BIGINT)inizio,
CAST(CASE WHEN d_fine='' THEN concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) ELSE CAST(CONCAT(SUBSTR(d_fine,1,4),SUBSTR(d_fine,6,2),SUBSTR(d_fine,9,2)) AS INT) END AS BIGINT) fine ,
CASE WHEN d_inizio='' THEN '' ELSE CAST(CONCAT(SUBSTR(d_inizio,1,4),SUBSTR(d_inizio,6,2),SUBSTR(d_inizio,9,2)) AS INT) END d_inizio_str,
CASE WHEN d_fine='' THEN '' ELSE CONCAT(SUBSTR(d_fine,1,4),SUBSTR(d_fine,6,2),SUBSTR(d_fine,9,2)) END AS d_fine_str,
codice_pod ,attivo,n_id_pod,n_id_fornitore,n_id_cliente,t_tipo_mercato,n_id_indirizzo,n_id_ind_forn,
CAST(concat( year(date_sub(current_date,${env:limit_gg})),lpad(month(date_sub(current_date,${env:limit_gg})),2,0),'01') as BIGINT) d_inizio_start
from 
(
 select n_id_fornitura,
 nvl(d_inizio_titolarita,'') as d_inizio,
 nvl(d_fine_titolarita,'') as d_fine,
 SUBSTR(t_codice_pod,1,14) codice_pod,
 CASE WHEN NVL(T_STATO_ATTIVAZIONE,'') = 'N' THEN '0' ELSE '1' END attivo,
 forn.n_id_pod,nvl(forn.n_id_fornitore,'')n_id_fornitore,forn.n_id_cliente,forn.t_tipo_mercato,n_id_indirizzo,n_id_ind_forn
 from 
 rcu.rcu_fornitura_p forn
 inner join rcu.rcu_pod_p pods on forn.n_id_pod = pods.n_id_pod
 inner join (select n_id_pod,max(d_aggiornamento),T_STATO_ATTIVAZIONE from rcu.rcu_pod_stato_p where T_STATO_ATTIVAZIONE ='A' OR NVL(T_STATO_ATTIVAZIONE,'')='' group by n_id_pod,T_STATO_ATTIVAZIONE) stato_pods on stato_pods.n_id_pod = pods.n_id_pod
 WHERE  isNumeric(n_id_fornitura) = true
 UNION ALL
 select n_id_fornitura,
 nvl(d_inizio_titolarita,'') as d_inizio,
 nvl(d_fine_titolarita,'') as d_fine,
 SUBSTR(t_codice_pod,1,14) codice_pod,'0' attivo,
 forn.n_id_pod,nvl(forn.n_id_fornitore,'')n_id_fornitore,forn.n_id_cliente,forn.t_tipo_mercato,n_id_indirizzo,n_id_ind_forn
 from 
 rcus.rcus_fornitura_p forn
 inner join rcu.rcu_pod_p pods on forn.n_id_pod = pods.n_id_pod
 inner join (select n_id_pod,max(d_aggiornamento),T_STATO_ATTIVAZIONE from rcus.rcus_podstato_p where b_valido='Y' group by n_id_pod,T_STATO_ATTIVAZIONE) stato_pods 
 on stato_pods.n_id_pod = pods.n_id_pod
 WHERE CONCAT(nvl(d_inizio_titolarita,''),nvl(d_fine_titolarita,'')) <> '' and isNumeric(n_id_fornitura) = true
 and b_valido='N'
)TBL_DATA
) as tbl 
where  fine >= cast(concat(year(date_sub(current_date,${env:limit_gg})),lpad(month(date_sub(current_date,${env:limit_gg})),2,0),'01') as bigint)  and (inizio+2) < fine
) AS TTX
where inizio = min_inizio;

create TABLE mongodbs.forniture stored as parquet as
select f.* from 
forniture_tmp f
inner join ( select concat(n_id_fornitura,max(fine))k_key
from forniture_tmp 
group by n_id_fornitura,inizio) t
on concat(f.n_id_fornitura,f.fine)=t.k_key;

create TABLE mongodbs.forniture_info stored as parquet as
SELECT forniture.n_id_fornitura,
forniture.n_id_pod,
forniture.n_id_cliente,
forniture.inizio d_inizio_titolarita,
forniture.fine d_fine_titolarita,
forniture.d_inizio_str d_inizio_titolarita_str,
forniture.d_fine_str d_fine_titolarita_str,
forniture.n_id_fornitore,
forniture.t_tipo_mercato,
forniture.n_id_indirizzo,
forniture.n_id_ind_forn,
forniture.codice_pod,
rcu_residenza.t_residente,
rcu_tariffa.t_tariffa_distr,
rcu_azienda.t_piva,
rcu_azienda.t_rag_soc
FROM mongodbs.forniture forniture
LEFT JOIN rcu.rcu_residenza_p AS rcu_residenza
ON forniture.n_id_fornitura=rcu_residenza.n_id_fornitura
AND rcu_residenza.b_valido='Y'
AND rcu_residenza.b_ultima='Y'
AND rcu_residenza.b_storico='O'
LEFT JOIN rcu.rcu_tariffa_p rcu_tariffa
ON forniture.n_id_fornitura=rcu_tariffa.n_id_fornitura
AND rcu_tariffa.b_valido='Y'
AND rcu_tariffa.b_ultima='Y'
AND rcu_tariffa.b_storico='O'
LEFT JOIN (select CAST(n_id_azienda AS STRING)n_id_azienda,t_rag_soc,t_piva from rcu.rcu_azienda_p where isNumeric(n_id_azienda)=true limit 2000) AS rcu_azienda
ON forniture.n_id_fornitore = rcu_azienda.n_id_azienda;
 
        


