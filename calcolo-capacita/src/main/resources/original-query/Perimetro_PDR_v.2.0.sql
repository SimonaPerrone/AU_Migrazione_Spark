/* Formatted on 31/03/2021 08:46:15 (QP5 v5.227.12220.39754) */
--create table tmp_clg_perimetro_pdr as

CREATE OR REPLACE FORCE VIEW MADDARII.CLG_PERIMETRO_PDR_GM_VIEW
(
   DATA_CALC,
   N_ID_PDR,
   T_CODICE_PDR,
   N_PRELIEVO_ANNUO,
   N_ID_REMI
)
AS
   WITH INPUT_DATA
        AS (SELECT TRUNC (TO_DATE ('2021/02/15', 'yyyy/mm/dd')) DATA_CALC --,TO_NUMBER(TO_CHAR(TO_DATE('2021/02/15','yyyy/mm/dd'),'YYYYMMDDHH24MISS')) EXECUTION_ID
                                                                         ,
                   CASE
                      WHEN TO_NUMBER (
                              TO_CHAR (
                                 TRUNC (TO_DATE ('2021/02/15', 'yyyy/mm/dd')),
                                 'MM')) BETWEEN 10
                                            AND 12
                      THEN
                         TO_CHAR (
                            ADD_MONTHS (
                               TRUNC (TO_DATE ('2021/02/15', 'yyyy/mm/dd')),
                               12),
                            'YYYY')
                      ELSE
                         TO_CHAR (
                            TRUNC (TO_DATE ('2021/02/15', 'yyyy/mm/dd')),
                            'YYYY')
                   END
                      ANNO
              FROM DUAL)
   SELECT D.DATA_CALC,
          TO_CHAR (PDR.N_ID_PDR) N_ID_PDR,
          TO_CHAR (PDR.T_CODICE_PDR) T_CODICE_PDR,
          PREL.N_PRELIEVO_ANNUO,
          TO_CHAR (CON.N_ID_REMI) N_ID_REMI
     FROM RCUGAS.RCUGAS_PDR PDR
          JOIN INPUT_DATA D ON 1 = 1
          JOIN
          RCUGAS.RCUGAS_CONNESSIONE CON
             ON     CON.N_ID_PDR = PDR.N_ID_PDR
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   CON.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   CON.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
          JOIN
          RCUGAS.RCUGAS_PDR_STATO PDR_STATO
             ON     PDR_STATO.N_ID_PDR = PDR.N_ID_PDR
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   PDR_STATO.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   PDR_STATO.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
                AND PDR_STATO.T_COD_STATO_PDR = 'P'
          -- Dati di prelievo
          JOIN
          MADDARII.RCUGAS_VAR_PROFILO_VIEW PROF
             ON     PROF.N_ID_PDR = PDR.N_ID_PDR
                AND TRUNC (D.DATA_CALC) BETWEEN PROF.D_DATA_INIZIO
                                            AND PROF.D_DATA_FINE
                AND d.anno = PROF.T_ANNO
          JOIN
          MADDARII.RCUGAS_VAR_TRATTAMENTO_VIEW TRATT
             ON     TRATT.N_ID_PDR = PDR.N_ID_PDR
                AND TRUNC (D.DATA_CALC) BETWEEN TRATT.D_DATA_INIZIO
                                            AND TRATT.D_DATA_FINE
                AND d.anno = TRATT.T_ANNO
          JOIN
          MADDARII.RCUGAS_VAR_PREL_ANNUO_VIEW PREL
             ON     PREL.N_ID_PDR = PDR.N_ID_PDR
                AND TRUNC (D.DATA_CALC) BETWEEN PREL.D_DATA_INIZIO
                                            AND PREL.D_DATA_FINE
                AND d.anno = PREL.T_ANNO
    WHERE     1 = 1
          AND NVL (TRATT.T_TRATTAMENTO_SETTLEMENT, 'Y') IN ('G', 'M')
          AND PROF.T_COD_CAT_USO IN ('T1', 'C2');