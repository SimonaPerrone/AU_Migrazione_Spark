create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

drop table mongodbs.forniture;
create TABLE mongodbs.forniture stored as parquet as
SELECT DISTINCT n_id_fornitura,
CASE WHEN inizio < d_inizio_start THEN d_inizio_start ELSE inizio end inizio,fine ,d_inizio_str,d_fine_str,codice_pod,attivo,n_id_pod,n_id_fornitore,t_tipo_mercato,n_id_cliente,n_id_indirizzo,n_id_ind_forn
 FROM (
select  n_id_fornitura,inizio,fine ,codice_pod,attivo,
n_id_pod,n_id_fornitore,n_id_cliente,t_tipo_mercato,nvl(n_id_indirizzo,'')n_id_indirizzo,nvl(n_id_ind_forn,'')n_id_ind_forn,
min(inizio) over ( partition by codice_pod,fine)min_inizio,d_inizio_str,d_fine_str,d_inizio_start
from (
select n_id_fornitura,
CAST(CASE WHEN d_inizio='' THEN concat( year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0),'01') ELSE CAST(CONCAT(SUBSTR(d_inizio,1,4),SUBSTR(d_inizio,6,2),SUBSTR(d_inizio,9,2)) AS INT) END AS BIGINT)inizio,
CAST(CASE WHEN d_fine='' THEN concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) ELSE CAST(CONCAT(SUBSTR(d_fine,1,4),SUBSTR(d_fine,6,2),SUBSTR(d_fine,9,2)) AS INT) END AS BIGINT) fine ,
CASE WHEN d_inizio='' THEN '' ELSE CAST(CONCAT(SUBSTR(d_inizio,1,4),SUBSTR(d_inizio,6,2),SUBSTR(d_inizio,9,2)) AS INT) END d_inizio_str,
CASE WHEN d_fine='' THEN '' ELSE CONCAT(SUBSTR(d_fine,1,4),SUBSTR(d_fine,6,2),SUBSTR(d_fine,9,2)) END AS d_fine_str,
codice_pod ,attivo,n_id_pod,n_id_fornitore,n_id_cliente,t_tipo_mercato,n_id_indirizzo,n_id_ind_forn,
CAST(concat( year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0),'01') as BIGINT) d_inizio_start
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
where  fine >= cast(concat(year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0),'01') as bigint)  and (inizio+2) < fine
) AS TTX
where inizio = min_inizio;

drop table mongodbs.forniture_info;
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

drop table mongodbs.switch;
create TABLE mongodbs.switch stored as parquet as
SELECT DISTINCT 
            SUBSTR(switch_1.t_codice_pod,1,14)t_codice_pod,
            case nvl(switch_1.d_data_decorrenza,'') when '' then cast(19700101 as bigint)
            else CAST(CONCAT(SUBSTR(switch_1.d_data_decorrenza,1,4),SUBSTR(switch_1.d_data_decorrenza,6,2),SUBSTR(switch_1.d_data_decorrenza,9,2)) AS BIGINT) end data_switch,
            switch_1.n_id_pratica,
            CASE
                WHEN t001_app_prt_pratiche.t_stato='INCORSO' or t001_app_prt_pratiche.t_stato='IN CORSO' THEN 'true'
                ELSE 'false'
                END AS switching_in_corso,
            switch_1.n_id_cliente_rcu n_id_cliente    
        FROM swtch.prt_se_p AS switch_1
        INNER JOIN (
            SELECT
                t_codice_pod,
                MAX(d_data_decorrenza) AS data_switch,
                MAX(d_data_contratto) AS m_data_contratto
            FROM swtch.prt_se_p
            where b_ammissibile='Y' AND b_invalidata <>'Y'
            GROUP BY t_codice_pod
        ) AS switch_2
        ON switch_1.t_codice_pod = switch_2.t_codice_pod
            AND switch_1.d_data_decorrenza = switch_2.data_switch AND d_data_contratto = m_data_contratto
        LEFT JOIN userappl.t001_app_prt_pratiche_p AS t001_app_prt_pratiche
        ON t001_app_prt_pratiche.n_id_pratica=switch_1.n_id_pratica;
        
    
drop table mongodbs.gdm;
create TABLE mongodbs.gdm stored as parquet as
SELECT     DISTINCT 
            rcu_pod_tecn_out.n_id_pod,
            SUBSTR(pods.t_codice_pod,1,14) codice_pod,
            rcu_pod_tecn_out.n_potenza_disponibile,
            rcu_pod_tecn_out.n_potenza_impegnata,
            rcu_pod_tecn_out.n_tensione,
            rcu_pod_tecn_out.t_tipo_misuratore,
            case nvl(rcu_pod_tecn_out.d_oper_misurator_att,'') when '' then cast(19700101 as bigint)
            else CAST(CONCAT(SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att,1,4),SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att,6,2),SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att,9,2)) AS BIGINT) end d_oper_misurator_att,
            nvl(rcu_pod_tecn_out.d_oper_misurator_att,'')d_oper_misurator_att_str,
            CASE when rcus_podtecn.n_id_pod  is null then '' ELSE 'SI' END AS cambio_GDM,
            CASE when rcus_podtecn.n_id_pod  is null then cast(19700101 as bigint)  else 
            CAST(CONCAT(SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,1,4),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,6,2),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,9,2)) AS BIGINT) end  AS data_cambio_GDM,            
            CASE when rcus_podtecn.n_id_pod  is null then '' else nvl(CONCAT(SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,1,4),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,6,2),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,9,2)),'') end data_cambio_GDM_str,
            dati_trattamento.trattamento,
            '' AS stato_misuratore_2g,
            rcu_pod_tecn_out.t_mat_misuratore_att,
            CAST(CONCAT(SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,1,4),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,6,2),SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att,9,2)) AS BIGINT) d_inst_misurator_att,
            year(date_add(cast(rcu_pod_tecn_out.d_oper_misurator_att as timestamp),396)) anno_start_misure_orarie,
            month(date_add(cast(rcu_pod_tecn_out.d_oper_misurator_att as timestamp),396)) mese_start_misure_orarie
        FROM (
            SELECT
                rcu_podtecn.n_id_pod,
                rcu_podtecn.n_potenza_disponibile,
                rcu_podtecn.n_potenza_impegnata,
                rcu_podtecn.n_tensione,
                rcu_podtecn.t_tipo_misuratore,
                rcu_podtecn.t_mat_misuratore_att,
                rcu_podtecn.d_oper_misurator_att,
                MAX(rcu_podtecn.d_inst_misurator_att) AS d_inst_misurator_att
            FROM rcu.rcu_pod_tecn_p AS rcu_podtecn
            GROUP BY
                rcu_podtecn.n_id_pod,
                rcu_podtecn.n_potenza_disponibile,
                rcu_podtecn.n_potenza_impegnata,
                rcu_podtecn.n_tensione,
                rcu_podtecn.t_tipo_misuratore,
                rcu_podtecn.t_mat_misuratore_att,
                rcu_podtecn.d_oper_misurator_att
        ) AS rcu_pod_tecn_out
        left JOIN (select rcus_podtecn_in.* from  rcus.rcus_podtecn_p rcus_podtecn_in
            join rcu.rcu_pod_tecn_p rcu_pod_tecn2
            ON rcus_podtecn_in.n_id_pod=rcu_pod_tecn2.n_id_pod
            AND rcus_podtecn_in.t_mat_misuratore_att IS NOT NULL
            where rcus_podtecn_in.t_mat_misuratore_att<>rcu_pod_tecn2.t_mat_misuratore_att) 
        AS rcus_podtecn on rcu_pod_tecn_out.n_id_pod = rcus_podtecn.n_id_pod
        LEFT JOIN (
            SELECT
                n_id_pod,d_anno_mese,
                CASE
                WHEN d_anno_mese < current_anno_mese THEN NVL((case when nvl(t_trattamento_succ,'')='' then null else t_trattamento_succ end),t_trattamento)
                WHEN d_anno_mese  >= current_anno_mese THEN NVL((case when nvl(t_trattamento,'')='' then null else t_trattamento end) ,t_trattamento_succ)
                ELSE NULL
                END AS trattamento
            FROM (select n_id_pod,CAST(CONCAT(SUBSTR(d_anno_mese,1,4),SUBSTR(d_anno_mese,6,2),SUBSTR(d_anno_mese,9,2)) AS BIGINT)d_anno_mese,t_trattamento_succ,t_trattamento,
            cast(concat( year(current_date),lpad(month(current_date),2,0),'01') as bigint) current_anno_mese
                   from rcu.rcu_pod_misure_p)  rcu_pod_misure
        ) AS dati_trattamento
        ON dati_trattamento.n_id_pod=rcu_pod_tecn_out.n_id_pod
        inner join rcu.rcu_pod_p pods on rcu_pod_tecn_out.n_id_pod = pods.n_id_pod;        
        
    
drop table mongodbs.RCU_POD_DISTR;
create TABLE mongodbs.RCU_POD_DISTR stored as parquet as
select 
            RCU_POD_DISTR.n_id_pod,
            RCU_AZIENDA.T_rag_soc 
        from RCU.RCU_POD_DISTR_p RCU_POD_DISTR 
        join (select CAST(n_id_azienda AS STRING)n_id_azienda,t_rag_soc T_rag_soc,t_piva from rcu.rcu_azienda_p where isNumeric(n_id_azienda)=true limit 2000) AS  RCU_AZIENDA
        on RCU_AZIENDA.n_id_azienda=NVL(RCU_POD_DISTR.n_id_distr,'');    
        



drop table mongodbs.pod;    
create TABLE mongodbs.pod stored as parquet as    
SELECT DISTINCT
        fornitura.n_id_pod,
        fornitura.codice_pod t_codice_pod,
        CASE
            WHEN fornitura.n_id_indirizzo IS NULL THEN fornitura.n_id_ind_forn
            ELSE fornitura.n_id_indirizzo
            END AS id_indirizzo,
        fornitura.n_id_fornitura,
        fornitura.n_id_cliente,
        fornitura.d_inizio_titolarita,
        fornitura.d_inizio_titolarita_str,
        fornitura.d_fine_titolarita,
        fornitura.d_fine_titolarita_str,
        CASE
            WHEN rcu_azienda.t_piva IN ('00000000012','00000000010','00000000011') THEN 'S'
            ELSE fornitura.t_tipo_mercato
            END AS tipo_mercato,
        fornitura.n_id_fornitore,
        fornitura.t_residente,
        fornitura.t_tariffa_distr AS tariffa,
        rcu_indirizzo.t_toponimo,
        rcu_indirizzo.t_nomestrada,
        rcu_indirizzo.t_civico,
        rcu_indirizzo.t_comune,
        rcu_indirizzo.t_cap,
        rcu_indirizzo.t_provincia,
        rcu_indirizzo.t_nazione,
        gdm.n_potenza_disponibile AS potenza_disponibile,
        gdm.n_potenza_impegnata AS potenza_impegnata,
        gdm.n_tensione AS tensione,
        gdm.t_tipo_misuratore AS tipo_misuratore,
        gdm_cambio.cambio_GDM,
        gdm_cambio.data_cambio_GDM,
        gdm_cambio.data_cambio_GDM_str,
        gdm_cambio.d_inst_misurator_att,
        gdm.stato_misuratore_2g,
        gdm.trattamento,
        switch.data_switch,
        switch.switching_in_corso,
        gdm.d_oper_misurator_att,
        gdm.d_oper_misurator_att_str,
        gdm.t_mat_misuratore_att matricola_misuratore,
        gdm.anno_start_misure_orarie,
        gdm.mese_start_misure_orarie,
        fornitura.t_piva,
        fornitura.t_rag_soc
    FROM mongodbs.forniture_info AS fornitura
    LEFT JOIN 
    (
     select switchx.*,forn.n_id_fornitura from mongodbs.switch  switchx
     inner join mongodbs.forniture_info forn on switchx.t_codice_pod=forn.codice_pod   and switchx.n_id_cliente = forn.n_id_cliente
     where switchx.data_switch >=forn.d_inizio_titolarita and switchx.data_switch <= (case d_fine_titolarita_str when '' then switchx.data_switch else forn.d_fine_titolarita end )
    ) AS switch
    ON switch.t_codice_pod=fornitura.codice_pod  and switch.n_id_fornitura= fornitura.n_id_fornitura
    LEFT JOIN rcu.rcu_indirizzo_p AS rcu_indirizzo ON COALESCE(fornitura.n_id_indirizzo,fornitura.n_id_ind_forn)=rcu_indirizzo.n_id
    LEFT JOIN 
    (
    select gdmx.*,forn.n_id_fornitura from mongodbs.gdm gdmx
    inner join mongodbs.forniture forn on gdmx.n_id_pod=forn.n_id_pod
    where nvl(gdmx.d_inst_misurator_att,0)=0 or (gdmx.d_inst_misurator_att <=forn.fine)
    ) AS gdm 
    ON gdm.n_id_pod=fornitura.n_id_pod and gdm.n_id_fornitura= fornitura.n_id_fornitura
    LEFT JOIN 
    (
    select gdmx.*,forn.n_id_fornitura from mongodbs.gdm gdmx
    inner join mongodbs.forniture forn on gdmx.n_id_pod=forn.n_id_pod
    where (gdmx.data_cambio_GDM >=forn.inizio and gdmx.data_cambio_GDM <=forn.fine) or 
          (gdmx.d_inst_misurator_att >=forn.inizio and gdmx.d_inst_misurator_att <=forn.fine)
    ) AS gdm_cambio 
    ON gdm_cambio.n_id_pod=fornitura.n_id_pod and gdm_cambio.n_id_fornitura= fornitura.n_id_fornitura
    LEFT JOIN (select CAST(n_id_azienda AS STRING)n_id_azienda,t_rag_soc,t_piva from rcu.rcu_azienda_p where isNumeric(n_id_azienda)=true limit 2000 ) AS rcu_azienda
    ON rcu_azienda.n_id_azienda = NVL(fornitura.n_id_fornitore,'');
    
    
    
drop table if exists mongodbs.fasce;

create TABLE mongodbs.fasce stored as parquet as     
SELECT n_id_pod,MAX(F_LUNEDI)F_LUNEDI,MAX(F_MARTEDI)F_MARTEDI,MAX(F_MERCOLEDI)F_MERCOLEDI,
MAX(F_GIOVEDI)F_GIOVEDI,MAX(F_VENERDI)F_VENERDI,MAX(F_SABATO)F_SABATO,MAX(F_DOMENICA)F_DOMENICA,MAX(F_FESTIVO)F_FESTIVO,
MAX(d_inizio_validita)d_inizio_validita,MAX(d_fine_validita)d_fine_validita,
MAX(d_fine_validita_str)d_fine_validita_str,MAX(d_data_iniziofreezing)d_data_iniziofreezing
FROM 
(
SELECT n_id_pod,
case F_LUNEDI when '1' then fasce else '' end F_LUNEDI ,
case F_MARTEDI when '1' then fasce else '' end F_MARTEDI,
case F_MERCOLEDI when '1' then fasce else '' end F_MERCOLEDI,
case F_GIOVEDI when '1' then fasce else '' end F_GIOVEDI,
case F_VENERDI when '1' then fasce else '' end F_VENERDI,
case F_SABATO when '1' then fasce else '' end F_SABATO,
case F_DOMENICA when '1' then fasce else '' end F_DOMENICA,
case F_FESTIVO when '1' then fasce else '' end F_FESTIVO,
CAST(CASE WHEN d_inizio_validita='' THEN concat( year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0),'01') ELSE CAST(CONCAT(SUBSTR(d_inizio_validita,1,4),SUBSTR(d_inizio_validita,6,2),SUBSTR(d_inizio_validita,9,2)) AS INT) END AS BIGINT)d_inizio_validita,
CAST(CASE WHEN d_fine_validita='' THEN concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) ELSE CAST(CONCAT(SUBSTR(d_fine_validita,1,4),SUBSTR(d_fine_validita,6,2),SUBSTR(d_fine_validita,9,2)) AS INT) END AS BIGINT) d_fine_validita ,
CASE WHEN d_fine_validita='' THEN '' ELSE CONCAT(SUBSTR(d_fine_validita,1,4),SUBSTR(d_fine_validita,6,2),SUBSTR(d_fine_validita,9,2)) END  d_fine_validita_str ,
CASE WHEN d_data_iniziofreezing='' THEN d_data_iniziofreezing ELSE CONCAT(SUBSTR(d_data_iniziofreezing,1,4),SUBSTR(d_data_iniziofreezing,6,2),SUBSTR(d_data_iniziofreezing,9,2))  END  d_data_iniziofreezing 
FROM(
SELECT
    misuratore.n_id_pod,
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
GROUP BY n_id_pod;

    
drop table mongodbs.forniture_elettriche;
create TABLE mongodbs.forniture_elettriche stored as parquet as    
SELECT DISTINCT
    nvl(rcu_clientefinale.t_cf_piva,'')t_cf,
    nvl(rcu_clientefinale.t_nome,'') t_nome,
    nvl(rcu_clientefinale.t_cognome,'') t_cognome,
    nvl(rcu_clientefinale.t_piva,'')t_piva,
    nvl(rcu_clientefinale.t_ragsoc,'') t_ragsoc,
    nvl(pod.t_codice_pod,'') AS codice_pod,
    nvl(pod.n_id_fornitura,'') AS codice_fornitura,
    nvl(pod.d_inizio_titolarita,0) AS data_inizio_fornitura_num,
    nvl(pod.d_inizio_titolarita_str,'') AS data_inizio_fornitura,
    nvl(pod.d_fine_titolarita_str,'') AS data_fine_fornitura,
    nvl(pod.d_fine_titolarita,0) AS data_fine_fornitura_num,
    nvl(pod.tipo_mercato,'') tipo_mercato,
    nvl(pod.t_residente,'') AS residente,
    nvl(pod.tariffa,'')tariffa,
    nvl(pod.tensione,'')tensione,
    nvl(pod.potenza_disponibile,'')potenza_disponibile,
    nvl(pod.potenza_impegnata,'')potenza_impegnata,
    nvl(pod.tipo_misuratore,'')tipo_misuratore,
    CASE WHEN pod.tipo_misuratore='G' then (
        CASE WHEN d_oper_misurator_att > cast(concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) as bigint) then 'IN FUNZIONE'
             WHEN CAST(SUBSTR(d_oper_misurator_att,1,6) AS INT) <= cast(concat( year(current_date),lpad(month(current_date),2,0)) as int) and 
                  cast(concat( year(current_date),lpad(month(current_date),2,0)) as int) <= cast(CONCAT(pod.anno_start_misure_orarie,lpad(pod.mese_start_misure_orarie,2,0)) as int) then 'ATTIVO'
             WHEN cast(concat( year(current_date),lpad(month(current_date),2,0)) as int) >     cast(CONCAT(pod.anno_start_misure_orarie,lpad(pod.mese_start_misure_orarie,2,0)) as int) AND pod.trattamento='O' THEN 'COMPLETAMENTE CONFIGURABILE' 
             ELSE '' END ) ELSE '' END AS stato_misuratore_2g,
    nvl(pod.t_toponimo,'') AS toponimo,
    nvl(pod.t_nomestrada,'') AS nome_strada,
    nvl(pod.t_civico,'') AS civico,
    nvl(pod.t_comune,'') AS comune,
    nvl(pod.t_cap,'') AS cap,
    nvl(pod.t_provincia,'') AS provincia,
    nvl(pod.t_nazione,'') AS nazione,
    nvl(pod.trattamento,'') trattamento,
    '' AS data_inizio_processo_gdm,
    '' AS data_fine_processo_gdm,
    nvl(pod.d_inst_misurator_att,'') AS data_inizio_validita_gdm,
    case nvl(pod.d_inst_misurator_att,'') when '' then '' else 'PRO001' end AS id_processo_gdm,
    CASE
        WHEN pod.data_cambio_GDM >= cast(concat( year(current_date),lpad(month(current_date),2,0),lpad(day(current_date),2,0)) as bigint) THEN 'true'
        ELSE 'false'
        END AS in_corso_gdm,
    case nvl(pod.d_inst_misurator_att,'') when '' then '' else 'note' end AS note_gdm,
    case nvl(pod.d_inst_misurator_att,'') when '' then '' else 'cambio_gdm' end AS tipo_processo_gdm,
    '' AS data_inizio_processo_switch,
    '' AS data_fine_processo_switch,
    case nvl(pod.data_switch,19700101) when 19700101 then '' else cast(pod.data_switch as string) end  AS data_inizio_validita_switch,
    case nvl(pod.data_switch,19700101) when 19700101 then '' else 'PRO002' end AS id_processo_switch,
    nvl(pod.switching_in_corso,'') AS in_corso_switch,
    case nvl(pod.data_switch,19700101) when 19700101 then '' else 'note' end AS note_switch,
    case nvl(pod.data_switch,19700101) when 19700101 then '' else 'switch' end AS tipo_processo_switch,
    nvl(pod.matricola_misuratore,'')matricola_misuratore,
    nvl(pod.t_piva,'') p_iva_cc ,
    nvl(pod.t_rag_soc,'') ragione_sociale_cc,
    nvl(RCU_POD_DISTR.t_rag_soc,'') ragione_sociale_distributore  ,
    nvl(fasce.F_LUNEDI,'')F_LUNEDI,
    nvl(fasce.F_MARTEDI,'')F_MARTEDI,
    nvl(fasce.F_MERCOLEDI,'')F_MERCOLEDI,
    nvl(fasce.F_GIOVEDI,'')F_GIOVEDI,
    nvl(fasce.F_VENERDI,'')F_VENERDI,
    nvl(fasce.F_SABATO,'')F_SABATO,
    nvl(fasce.F_DOMENICA,'')F_DOMENICA,
    nvl(fasce.F_FESTIVO,'')F_FESTIVO,
    nvl(fasce.d_inizio_validita,'')d_inizio_validita_fascia,
    nvl(fasce.d_fine_validita_str,'')d_fine_validita_fascia,
    nvl(fasce.d_data_iniziofreezing,'')d_data_iniziofreezing
FROM (  select *,case  nvl(t_cf,'') when '' then t_piva else t_cf end t_cf_piva
        from rcu.rcu_clientefinale_p 
        where concat(nvl(t_cf,''),nvl(t_piva,''))<>'' 
        )  rcu_clientefinale
 JOIN  mongodbs.pod AS pod
 ON rcu_clientefinale.n_id_cliente=pod.n_id_cliente
 left join  mongodbs.RCU_POD_DISTR  RCU_POD_DISTR
 on pod.n_id_pod= RCU_POD_DISTR.n_id_pod
 left outer join 
 ( 
 select forn.n_id_fornitura,fasce_in.* from mongodbs.fasce fasce_in left outer join mongodbs.forniture forn on fasce_in.n_id_pod=forn.n_id_pod
 where forn.n_id_fornitura is not null and fasce_in.d_inizio_validita >=forn.inizio and fasce_in.d_fine_validita <= forn.fine 
 ) fasce on fasce.n_id_pod = pod.n_id_pod and fasce.n_id_fornitura = pod.n_id_fornitura;
 
TRUNCATE TABLE misure.prt_tmo_mn_f;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT INTO misure.prt_tmo_mn_f PARTITION(annomese) 
                     SELECT CAST(eam AS DOUBLE)eam ,
                     CAST(SUBSTR(regexp_replace(d_ricezione,'-',''),1,8) AS BIGINT) data_ricezione,
                     CAST(SUBSTR(regexp_replace(datamisura,'-',''),1,8) AS BIGINT) data_lettura,
                     CAST(giornomisura AS INT)giornomisura,
                     SUBSTR(prt_tmo_mn.codice_pod,1,14)codice_pod,tipodato,CAST(perdita AS DOUBLE)perdita,
                     CAST(eaf1 AS DOUBLE)eaf1,CAST(eaf2 AS DOUBLE)eaf2,CAST(eaf3 AS DOUBLE)eaf3,cod_flusso,motivazione,validato,
                     CAST(annomese AS INT)annomese
                     FROM tmpod.prt_tmo_mn_p prt_tmo_mn
                     INNER JOIN (SELECT DISTINCT codice_pod FROM mongodbs.forniture) pods on pods.codice_pod = SUBSTR(prt_tmo_mn.codice_pod,1,14)
                     where  CAST(SUBSTR(regexp_replace(datamisura,'-',''),1,6) AS INT) >=cast(concat(year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0)) as INT)
                     AND (cod_flusso='RNO' OR (tipodato ='E' AND cod_flusso ='PNO' AND validato ='S'));

TRUNCATE TABLE misure.prt_tmo_mv_f;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT INTO misure.prt_tmo_mv_f PARTITION(annomese) 
                     SELECT CAST(eam AS DOUBLE)eam ,unix_timestamp(datarilevazione)d_rilevazione_number,
                     CAST(SUBSTR(regexp_replace(datarilevazione,'-',''),1,8) AS BIGINT) datarilevazione,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,8) AS BIGINT) data_voltura,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),7,2) AS INT)giorno_voltura,
                     SUBSTR(prt_tmo_mv.codice_pod,1,14)codice_pod,tipodato,
                     CAST(eaf1 AS DOUBLE)eaf1,CAST(eaf2 AS DOUBLE)eaf2,CAST(eaf3 AS DOUBLE)eaf3,cod_flusso,VALIDATO,
                     CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,6) AS INT)annomese
                     FROM tmpod.prt_tmo_mv_p prt_tmo_mv
                     INNER JOIN (SELECT DISTINCT codice_pod FROM mongodbs.forniture) pods on pods.codice_pod = SUBSTR(prt_tmo_mv.codice_pod,1,14)
                     where  CAST(SUBSTR(regexp_replace(datavoltura,'-',''),1,6) AS INT) >=cast(concat(year(date_sub(current_date,396)),lpad(month(date_sub(current_date,396)),2,0)) as INT) 
                     AND (cod_flusso='RNV' OR (tipodato ='E' AND cod_flusso ='VNO' AND validato ='S'));


