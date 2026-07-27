-- Define temporary macros
CREATE TEMPORARY MACRO isNumber(s STRING) CAST(s AS DOUBLE) IS NOT NULL;
CREATE TEMPORARY MACRO isNumeric(s STRING) s NOT RLIKE '[^0-9]';

-- Drop the existing table if it exists
DROP TABLE mongodbs.gdm;

-- Create the `gdm` table
CREATE TABLE mongodbs.gdm STORED AS PARQUET AS
SELECT DISTINCT 
    rcu_pod_tecn_out.n_id_pod,
    SUBSTR(pods.t_codice_pod, 1, 14) AS codice_pod,
    rcu_pod_tecn_out.n_potenza_disponibile,
    rcu_pod_tecn_out.n_potenza_impegnata,
    rcu_pod_tecn_out.n_tensione,
    rcu_pod_tecn_out.t_tipo_misuratore,
    CASE 
        WHEN NVL(rcu_pod_tecn_out.d_oper_misurator_att, '') = '' THEN CAST(19700101 AS BIGINT)
        ELSE CAST(CONCAT(
            SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att, 1, 4),
            SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att, 6, 2),
            SUBSTR(rcu_pod_tecn_out.d_oper_misurator_att, 9, 2)
        ) AS BIGINT)
    END AS d_oper_misurator_att,
    NVL(rcu_pod_tecn_out.d_oper_misurator_att, '') AS d_oper_misurator_att_str,
    CASE 
        WHEN rcus_podtecn.n_id_pod IS NULL THEN '' 
        ELSE 'SI' 
    END AS cambio_GDM,
    CASE 
        WHEN rcus_podtecn.n_id_pod IS NULL THEN CAST(19700101 AS BIGINT)
        ELSE CAST(CONCAT(
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 1, 4),
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 6, 2),
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 9, 2)
        ) AS BIGINT)
    END AS data_cambio_GDM,
    CASE 
        WHEN rcus_podtecn.n_id_pod IS NULL THEN '' 
        ELSE NVL(CONCAT(
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 1, 4),
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 6, 2),
            SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 9, 2)
        ), '') 
    END AS data_cambio_GDM_str,
    dati_trattamento.trattamento,
    '' AS stato_misuratore_2g,
    rcu_pod_tecn_out.t_mat_misuratore_att,
    CAST(CONCAT(
        SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 1, 4),
        SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 6, 2),
        SUBSTR(rcu_pod_tecn_out.d_inst_misurator_att, 9, 2)
    ) AS BIGINT) AS d_inst_misurator_att,
    YEAR(DATE_ADD(CAST(rcu_pod_tecn_out.d_oper_misurator_att AS TIMESTAMP), 370)) AS anno_start_misure_orarie,
    CASE 
        WHEN (MONTH(DATE_ADD(CAST(rcu_pod_tecn_out.d_oper_misurator_att AS TIMESTAMP), 365)) + 1) > 12 
        THEN 1 
        ELSE (MONTH(DATE_ADD(CAST(rcu_pod_tecn_out.d_oper_misurator_att AS TIMESTAMP), 365)) + 1) 
    END AS mese_start_misure_orarie
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
LEFT JOIN (
    SELECT rcus_podtecn_in.*
    FROM rcus.rcus_podtecn_p rcus_podtecn_in
    JOIN rcu.rcu_pod_tecn_p rcu_pod_tecn2
    ON rcus_podtecn_in.n_id_pod = rcu_pod_tecn2.n_id_pod
    AND rcus_podtecn_in.t_mat_misuratore_att IS NOT NULL
    WHERE rcus_podtecn_in.t_mat_misuratore_att <> rcu_pod_tecn2.t_mat_misuratore_att
) AS rcus_podtecn 
ON rcu_pod_tecn_out.n_id_pod = rcus_podtecn.n_id_pod
LEFT JOIN (
    SELECT
        n_id_pod,
        d_anno_mese,
        CASE
            WHEN d_anno_mese < current_anno_mese 
            THEN NVL(CASE 
                WHEN NVL(t_trattamento_succ, '') = '' THEN NULL 
                ELSE t_trattamento_succ 
            END, t_trattamento)
            WHEN d_anno_mese >= current_anno_mese 
            THEN NVL(CASE 
                WHEN NVL(t_trattamento, '') = '' THEN NULL 
                ELSE t_trattamento 
            END, t_trattamento_succ)
            ELSE NULL
        END AS trattamento
    FROM (
        SELECT 
            n_id_pod,
            CAST(CONCAT(SUBSTR(d_anno_mese, 1, 4), SUBSTR(d_anno_mese, 6, 2), SUBSTR(d_anno_mese, 9, 2)) AS BIGINT) AS d_anno_mese,
            t_trattamento_succ,
            t_trattamento,
            CAST(CONCAT(YEAR(CURRENT_DATE), LPAD(MONTH(CURRENT_DATE), 2, 0), '01') AS BIGINT) AS current_anno_mese
        FROM rcu.rcu_pod_misure_p
    ) AS rcu_pod_misure
) AS dati_trattamento
ON dati_trattamento.n_id_pod = rcu_pod_tecn_out.n_id_pod
INNER JOIN rcu.rcu_pod_p pods 
ON rcu_pod_tecn_out.n_id_pod = pods.n_id_pod;
