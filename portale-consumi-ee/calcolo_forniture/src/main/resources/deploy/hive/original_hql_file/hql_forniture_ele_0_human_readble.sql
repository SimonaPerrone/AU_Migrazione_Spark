-- Define temporary macros
CREATE TEMPORARY MACRO isNumber(s STRING) CAST(s AS DOUBLE) IS NOT NULL;
CREATE TEMPORARY MACRO isNumeric(s STRING) s NOT RLIKE '[^0-9]';

-- Drop existing tables
DROP TABLE mongodbs.forniture;
DROP TABLE mongodbs.forniture_info;

-- Create a temporary table with distinct records
CREATE TEMPORARY TABLE forniture_tmp STORED AS PARQUET AS
SELECT DISTINCT
    n_id_fornitura,
    CASE
        WHEN inizio < d_inizio_start THEN d_inizio_start
        ELSE inizio
    END AS inizio,
    fine,
    d_inizio_str,
    d_fine_str,
    codice_pod,
    attivo,
    n_id_pod,
    n_id_fornitore,
    t_tipo_mercato,
    n_id_cliente,
    n_id_indirizzo,
    n_id_ind_forn,
    t_servizio_tutela_sii
FROM (
    SELECT
        n_id_fornitura,
        inizio,
        fine,
        codice_pod,
        attivo,
        n_id_pod,
        n_id_fornitore,
        n_id_cliente,
        t_tipo_mercato,
        NVL(n_id_indirizzo, '') AS n_id_indirizzo,
        NVL(n_id_ind_forn, '') AS n_id_ind_forn,
        MIN(inizio) OVER (PARTITION BY codice_pod, fine) AS min_inizio,
        d_inizio_str,
        d_fine_str,
        d_inizio_start,
        t_servizio_tutela_sii
    FROM (
        SELECT
            n_id_fornitura,
            CAST(
                CASE
                    WHEN d_inizio = '' THEN CONCAT(YEAR(DATE_SUB(CURRENT_DATE, 1126)), LPAD(MONTH(DATE_SUB(CURRENT_DATE, 1126)), 2, 0), '01')
                    ELSE CAST(CONCAT(SUBSTR(d_inizio, 1, 4), SUBSTR(d_inizio, 6, 2), SUBSTR(d_inizio, 9, 2)) AS INT)
                END AS BIGINT
            ) AS inizio,
            CAST(
                CASE
                    WHEN d_fine = '' THEN CONCAT(YEAR(CURRENT_DATE), LPAD(MONTH(CURRENT_DATE), 2, 0), LPAD(DAY(CURRENT_DATE), 2, 0))
                    ELSE CAST(CONCAT(SUBSTR(d_fine, 1, 4), SUBSTR(d_fine, 6, 2), SUBSTR(d_fine, 9, 2)) AS INT)
                END AS BIGINT
            ) AS fine,
            CASE
                WHEN d_inizio = '' THEN ''
                ELSE CAST(CONCAT(SUBSTR(d_inizio, 1, 4), SUBSTR(d_inizio, 6, 2), SUBSTR(d_inizio, 9, 2)) AS INT)
            END AS d_inizio_str,
            CASE
                WHEN d_fine = '' THEN ''
                ELSE CONCAT(SUBSTR(d_fine, 1, 4), SUBSTR(d_fine, 6, 2), SUBSTR(d_fine, 9, 2))
            END AS d_fine_str,
            codice_pod,
            attivo,
            n_id_pod,
            n_id_fornitore,
            n_id_cliente,
            t_tipo_mercato,
            n_id_indirizzo,
            n_id_ind_forn,
            CAST(CONCAT(YEAR(DATE_SUB(CURRENT_DATE, 1126)), LPAD(MONTH(DATE_SUB(CURRENT_DATE, 1126)), 2, 0), '01') AS BIGINT) AS d_inizio_start,
            t_servizio_tutela_sii
        FROM (
            SELECT
                n_id_fornitura,
                NVL(d_inizio_titolarita, '') AS d_inizio,
                NVL(d_fine_titolarita, '') AS d_fine,
                SUBSTR(t_codice_pod, 1, 14) AS codice_pod,
                CASE
                    WHEN NVL(T_STATO_ATTIVAZIONE, '') = 'N' THEN '0'
                    ELSE '1'
                END AS attivo,
                forn.n_id_pod,
                NVL(forn.n_id_fornitore, '') AS n_id_fornitore,
                forn.n_id_cliente,
                forn.t_tipo_mercato,
                n_id_indirizzo,
                n_id_ind_forn,
                NVL(forn.t_servizio_tutela_sii, '') AS t_servizio_tutela_sii
            FROM
                rcu.rcu_fornitura_p forn
            INNER JOIN
                rcu.rcu_pod_p pods ON forn.n_id_pod = pods.n_id_pod
            INNER JOIN (
                SELECT
                    n_id_pod, MAX(d_aggiornamento), T_STATO_ATTIVAZIONE
                FROM
                    rcu.rcu_pod_stato_p
                WHERE
                    T_STATO_ATTIVAZIONE = 'A' OR NVL(T_STATO_ATTIVAZIONE, '') = ''
                GROUP BY
                    n_id_pod, T_STATO_ATTIVAZIONE
            ) stato_pods ON stato_pods.n_id_pod = pods.n_id_pod
            WHERE
                isNumeric(n_id_fornitura) = TRUE
            UNION ALL
            SELECT
                n_id_fornitura,
                NVL(d_inizio_titolarita, '') AS d_inizio,
                NVL(d_fine_titolarita, '') AS d_fine,
                SUBSTR(t_codice_pod, 1, 14) AS codice_pod,
                '0' AS attivo,
                forn.n_id_pod,
                NVL(forn.n_id_fornitore, '') AS n_id_fornitore,
                forn.n_id_cliente,
                forn.t_tipo_mercato,
                n_id_indirizzo,
                n_id_ind_forn,
                '' AS t_servizio_tutela_sii
            FROM
                rcus.rcus_fornitura_p forn
            INNER JOIN
                rcu.rcu_pod_p pods ON forn.n_id_pod = pods.n_id_pod
            INNER JOIN (
                SELECT
                    n_id_pod, MAX(d_aggiornamento), T_STATO_ATTIVAZIONE
                FROM
                    rcus.rcus_podstato_p
                WHERE
                    b_valido = 'Y'
                GROUP BY
                    n_id_pod, T_STATO_ATTIVAZIONE
            ) stato_pods ON stato_pods.n_id_pod = pods.n_id_pod
            WHERE
                CONCAT(NVL(d_inizio_titolarita, ''), NVL(d_fine_titolarita, '')) <> ''
                AND isNumeric(n_id_fornitura) = TRUE
                AND b_valido = 'N'
        ) TBL_DATA
    ) AS tbl
    WHERE
        fine >= CAST(CONCAT(YEAR(DATE_SUB(CURRENT_DATE, 1126)), LPAD(MONTH(DATE_SUB(CURRENT_DATE, 1126)), 2, 0), '01') AS BIGINT)
        AND (inizio + 2) < fine
) AS TTX
WHERE
    inizio = min_inizio;

-- Create the main forniture table
CREATE TABLE mongodbs.forniture STORED AS PARQUET AS
SELECT f.*
FROM forniture_tmp f
INNER JOIN (
    SELECT CONCAT(n_id_fornitura, MAX(fine)) AS k_key
    FROM forniture_tmp
    GROUP BY n_id_fornitura, inizio
) t ON CONCAT(f.n_id_fornitura, f.fine) = t.k_key;

-- Create the forniture_info table
CREATE TABLE mongodbs.forniture_info STORED AS PARQUET AS
SELECT
    forniture.n_id_fornitura,
    forniture.n_id_pod,
    forniture.n_id_cliente,
    forniture.inizio AS d_inizio_titolarita,
    forniture.fine AS d_fine_titolarita,
    forniture.d_inizio_str AS d_inizio_titolarita_str,
    forniture.d_fine_str AS d_fine_titolarita_str,
    forniture.n_id_fornitore,
    forniture.t_tipo_mercato,
    forniture.n_id_indirizzo,
    forniture.n_id_ind_forn,
    forniture.codice_pod,
    rcu_residenza.t_residente,
    rcu_tariffa.t_tariffa_distr,
    rcu_azienda.t_piva,
    rcu_azienda.t_rag_soc,
    forniture.t_servizio_tutela_sii
FROM mongodbs.forniture forniture
LEFT JOIN rcu.rcu_residenza_p AS rcu_residenza
    ON forniture.n_id_fornitura = rcu_residenza.n_id_fornitura
    AND rcu_residenza.b_valido = 'Y'
    AND rcu_residenza.b_ultima = 'Y'
    AND rcu_residenza.b_storico = 'O'
LEFT JOIN rcu.rcu_tariffa_p AS rcu_tariffa
    ON forniture.n_id_fornitura = rcu_tariffa.n_id_fornitura
    AND rcu_tariffa.b_valido = 'Y'
    AND rcu_tariffa.b_ultima = 'Y'
    AND rcu_tariffa.b_storico = 'O'
LEFT JOIN (
    SELECT
        CAST(n_id_azienda AS STRING) AS n_id_azienda,
        t_rag_soc,
        t_piva
    FROM rcu.rcu_azienda_p
    WHERE isNumeric(n_id_azienda) = TRUE
) AS rcu_azienda
    ON forniture.n_id_fornitore = rcu_azienda.n_id_azienda;
