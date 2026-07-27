create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

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
            where nvl(b_ammissibile,'')='Y' AND nvl(b_invalidata,'') <>'Y' 
            GROUP BY t_codice_pod
        ) AS switch_2
        ON switch_1.t_codice_pod = switch_2.t_codice_pod
            AND switch_1.d_data_decorrenza = switch_2.data_switch AND d_data_contratto = m_data_contratto
        LEFT JOIN userappl.t001_app_prt_pratiche_p AS t001_app_prt_pratiche
        ON t001_app_prt_pratiche.n_id_pratica=switch_1.n_id_pratica;
