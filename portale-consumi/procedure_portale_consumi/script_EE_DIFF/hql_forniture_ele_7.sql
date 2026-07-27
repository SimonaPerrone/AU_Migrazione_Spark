create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

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
    nvl(fasce.d_data_iniziofreezing,'')d_data_iniziofreezing,
    nvl(fasce.n_id_misuratore,'') id_misuratore_fasce
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
 where forn.n_id_fornitura is not null and fasce_in.d_fine_validita >=forn.inizio and fasce_in.d_fine_validita <= forn.fine 
 ) fasce on fasce.n_id_pod = pod.n_id_pod and fasce.n_id_fornitura = pod.n_id_fornitura
 order by codice_pod;

 
