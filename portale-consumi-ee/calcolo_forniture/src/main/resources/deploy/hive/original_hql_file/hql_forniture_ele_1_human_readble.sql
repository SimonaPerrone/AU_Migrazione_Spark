-- Define temporary macros
CREATE TEMPORARY MACRO isNumber(s STRING) CAST(s AS DOUBLE) IS NOT NULL;
CREATE TEMPORARY MACRO isNumeric(s STRING) s NOT RLIKE '[^0-9]';

-- Drop the existing table if it exists
DROP TABLE mongodbs.switch;

-- Create the `switch` table
CREATE TABLE mongodbs.switch STORED AS PARQUET AS
SELECT DISTINCT 
    SUBSTR(switch_1.t_codice_pod, 1, 14) AS t_codice_pod,
    CASE 
        WHEN NVL(switch_1.d_data_decorrenza, '') = '' THEN CAST(19700101 AS BIGINT)
        ELSE CAST(CONCAT(
            SUBSTR(switch_1.d_data_decorrenza, 1, 4),
            SUBSTR(switch_1.d_data_decorrenza, 6, 2),
            SUBSTR(switch_1.d_data_decorrenza, 9, 2)
        ) AS BIGINT)
    END AS data_switch,
    switch_1.n_id_pratica,
    CASE
        WHEN t001_app_prt_pratiche.t_stato = 'INCORSO' OR t001_app_prt_pratiche.t_stato = 'IN CORSO' THEN 'true'
        ELSE 'false'
    END AS switching_in_corso,
    switch_1.n_id_cliente_rcu AS n_id_cliente
FROM swtch.prt_se_p AS switch_1
INNER JOIN (
    SELECT
        t_codice_pod,
        MAX(d_data_decorrenza) AS data_switch,
        MAX(d_data_contratto) AS m_data_contratto
    FROM swtch.prt_se_p
    WHERE NVL(b_ammissibile, '') = 'Y' AND NVL(b_invalidata, '') <> 'Y'
    GROUP BY t_codice_pod
) AS switch_2
ON switch_1.t_codice_pod = switch_2.t_codice_pod
   AND switch_1.d_data_decorrenza = switch_2.data_switch
   AND switch_1.d_data_contratto = switch_2.m_data_contratto
LEFT JOIN userappl.t001_app_prt_pratiche_p AS t001_app_prt_pratiche
ON t001_app_prt_pratiche.n_id_pratica = switch_1.n_id_pratica;
