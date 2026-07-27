create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

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
            year(date_add(cast(rcu_pod_tecn_out.d_oper_misurator_att as timestamp),370)) anno_start_misure_orarie,
            CASE WHEN (month(date_add(cast(rcu_pod_tecn_out.d_oper_misurator_att as timestamp),365)) + 1) > 12 THEN 1 ELSE (month(date_add(cast(rcu_pod_tecn_out.d_oper_misurator_att as timestamp),365)) + 1) END   mese_start_misure_orarie
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
