-- Define temporary macros
CREATE TEMPORARY MACRO isNumber(s STRING) CAST(s AS DOUBLE) IS NOT NULL;
CREATE TEMPORARY MACRO isNumeric(s STRING) s NOT RLIKE '[^0-9]';

-- Drop the existing table if it exists
DROP TABLE IF EXISTS mongodbs.fasce;

-- Create the `fasce` table
CREATE TABLE mongodbs.fasce STORED AS PARQUET AS
SELECT 
    n_id_pod,
    n_id_misuratore,
    MAX(F_LUNEDI) AS F_LUNEDI,
    MAX(F_MARTEDI) AS F_MARTEDI,
    MAX(F_MERCOLEDI) AS F_MERCOLEDI,
    MAX(F_GIOVEDI) AS F_GIOVEDI,
    MAX(F_VENERDI) AS F_VENERDI,
    MAX(F_SABATO) AS F_SABATO,
    MAX(F_DOMENICA) AS F_DOMENICA,
    MAX(F_FESTIVO) AS F_FESTIVO,
    MAX(d_inizio_validita) AS d_inizio_validita,
    MAX(d_fine_validita) AS d_fine_validita,
    MAX(d_fine_validita_str) AS d_fine_validita_str,
    MAX(d_data_iniziofreezing) AS d_data_iniziofreezing
FROM (
    SELECT 
        n_id_pod,
        n_id_misuratore,
        CASE F_LUNEDI WHEN '1' THEN fasce ELSE '' END AS F_LUNEDI,
        CASE F_MARTEDI WHEN '1' THEN fasce ELSE '' END AS F_MARTEDI,
        CASE F_MERCOLEDI WHEN '1' THEN fasce ELSE '' END AS F_MERCOLEDI,
        CASE F_GIOVEDI WHEN '1' THEN fasce ELSE '' END AS F_GIOVEDI,
        CASE F_VENERDI WHEN '1' THEN fasce ELSE '' END AS F_VENERDI,
        CASE F_SABATO WHEN '1' THEN fasce ELSE '' END AS F_SABATO,
        CASE F_DOMENICA WHEN '1' THEN fasce ELSE '' END AS F_DOMENICA,
        CASE F_FESTIVO WHEN '1' THEN fasce ELSE '' END AS F_FESTIVO,
        CAST(
            CASE 
                WHEN NVL(d_inizio_validita, '') = '' 
                THEN CONCAT(YEAR(DATE_SUB(CURRENT_DATE, 1126)), LPAD(MONTH(DATE_SUB(CURRENT_DATE, 1126)), 2, 0), '01') 
                ELSE CAST(CONCAT(SUBSTR(d_inizio_validita, 1, 4), SUBSTR(d_inizio_validita, 6, 2), SUBSTR(d_inizio_validita, 9, 2)) AS INT) 
            END AS BIGINT
        ) AS d_inizio_validita,
        CAST(
            CASE 
                WHEN NVL(d_fine_validita, '') = '' 
                THEN CONCAT(YEAR(CURRENT_DATE), LPAD(MONTH(CURRENT_DATE), 2, 0), LPAD(DAY(CURRENT_DATE), 2, 0)) 
                ELSE CAST(CONCAT(SUBSTR(d_fine_validita, 1, 4), SUBSTR(d_fine_validita, 6, 2), SUBSTR(d_fine_validita, 9, 2)) AS INT) 
            END AS BIGINT
        ) AS d_fine_validita,
        CASE 
            WHEN NVL(d_fine_validita, '') = '' THEN '' 
            ELSE CONCAT(SUBSTR(d_fine_validita, 1, 4), SUBSTR(d_fine_validita, 6, 2), SUBSTR(d_fine_validita, 9, 2)) 
        END AS d_fine_validita_str,
        CASE 
            WHEN NVL(d_data_iniziofreezing, '') = '' THEN d_data_iniziofreezing 
            ELSE CONCAT(SUBSTR(d_data_iniziofreezing, 1, 4), SUBSTR(d_data_iniziofreezing, 6, 2), SUBSTR(d_data_iniziofreezing, 9, 2)) 
        END AS d_data_iniziofreezing
    FROM (
        SELECT 
            misuratore.n_id_pod,
            n_id_misuratore,
            CASE n_cod_giorno_2g WHEN '1' THEN '1' ELSE '0' END AS F_LUNEDI,
            CASE n_cod_giorno_2g WHEN '2' THEN '1' ELSE '0' END AS F_MARTEDI,
            CASE n_cod_giorno_2g WHEN '3' THEN '1' ELSE '0' END AS F_MERCOLEDI,
            CASE n_cod_giorno_2g WHEN '4' THEN '1' ELSE '0' END AS F_GIOVEDI,
            CASE n_cod_giorno_2g WHEN '5' THEN '1' ELSE '0' END AS F_VENERDI,
            CASE n_cod_giorno_2g WHEN '6' THEN '1' ELSE '0' END AS F_SABATO,
            CASE n_cod_giorno_2g WHEN '7' THEN '1' ELSE '0' END AS F_DOMENICA,
            CASE n_cod_giorno_2g WHEN '8' THEN '1' ELSE '0' END AS F_FESTIVO,
            CONCAT(
                NVL(fasce2.n_fine_fascia_1, ''), '-', NVL(fasce2.n_fascia_1, ''), ',',
                NVL(fasce2.n_fine_fascia_2, ''), '-', NVL(fasce2.n_fascia_2, ''), ',',
                NVL(fasce2.n_fine_fascia_3, ''), '-', NVL(fasce2.n_fascia_3, ''), ',',
                NVL(fasce2.n_fine_fascia_4, ''), '-', NVL(fasce2.n_fascia_4, ''), ',',
                NVL(fasce2.n_fine_fascia_5, ''), '-', NVL(fasce2.n_fascia_5, ''), ',',
                NVL(fasce2.n_fine_fascia_6, ''), '-', NVL(fasce2.n_fascia_6, '')
            ) AS fasce,
            d_inizio_validita,
            d_fine_validita,
            d_data_iniziofreezing
        FROM (
            SELECT 
                CASE n_fascia_1 WHEN '' THEN NULL ELSE n_fascia_1 END AS n_fascia_1,
                CASE n_fascia_2 WHEN '' THEN NULL ELSE n_fascia_2 END AS n_fascia_2,
                CASE n_fascia_3 WHEN '' THEN NULL ELSE n_fascia_3 END AS n_fascia_3,
                CASE n_fascia_4 WHEN '' THEN NULL ELSE n_fascia_4 END AS n_fascia_4,
                CASE n_fascia_5 WHEN '' THEN NULL ELSE n_fascia_5 END AS n_fascia_5,
                CASE n_fascia_6 WHEN '' THEN NULL ELSE n_fascia_6 END AS n_fascia_6,
                CASE n_fine_fascia_1 WHEN '' THEN NULL ELSE n_fine_fascia_1 END AS n_fine_fascia_1,
                CASE n_fine_fascia_2 WHEN '' THEN NULL ELSE n_fine_fascia_2 END AS n_fine_fascia_2,
                CASE n_fine_fascia_3 WHEN '' THEN NULL ELSE n_fine_fascia_3 END AS n_fine_fascia_3,
                CASE n_fine_fascia_4 WHEN '' THEN NULL ELSE n_fine_fascia_4 END AS n_fine_fascia_4,
                CASE n_fine_fascia_5 WHEN '' THEN NULL ELSE n_fine_fascia_5 END AS n_fine_fascia_5,
                CASE n_fine_fascia_6 WHEN '' THEN NULL ELSE n_fine_fascia_6 END AS n_fine_fascia_6,
                n_cod_giorno_2g,
                n_id_misuratore,
                d_aggiornamento,
                MAX(d_aggiornamento) OVER (PARTITION BY n_id_misuratore, n_cod_giorno_2g) AS max_aggiornamento
            FROM RCU.RCU_FASCE_MISURATORE_2G_p 
            WHERE isNumeric(n_id_misuratore)
        ) fasce2
        LEFT OUTER JOIN (
            SELECT 
                n_id_pod,
                d_inizio_validita,
                d_fine_validita,
                d_data_iniziofreezing,
                n_id_misuratore_2g
            FROM RCU.RCU_MISURATORE_2G_p 
            WHERE isNumeric(n_id_misuratore_2g) AND isNumeric(n_id_pod)
        ) misuratore 
        ON SUBSTR(misuratore.n_id_misuratore_2g, 1, 18) = SUBSTR(fasce2.n_id_misuratore, 1, 18)
        WHERE d_aggiornamento = max_aggiornamento AND n_id_pod IS NOT NULL
    ) AS TTX
) AS FFX
GROUP BY n_id_pod, n_id_misuratore;
