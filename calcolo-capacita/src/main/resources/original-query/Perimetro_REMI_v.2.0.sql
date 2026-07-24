/* Formatted on 31/03/2021 08:45:57 (QP5 v5.227.12220.39754) */
--DROP VIEW MADDARII.CLG_PERIMETRO_REMI_GM_VIEW;

CREATE OR REPLACE FORCE VIEW MADDARII.CLG_PERIMETRO_REMI_GM_VIEW
(
   N_ID_REMI,
   REMI_POOL,
   N_ID_REMI_ANAGRAFICA,
   DATA_CALC,
   T_TARIFFA,
   T_Z,
   T_PMAX
)
AS
   WITH INPUT_DATA
        AS (SELECT TRUNC (TO_DATE ('2021/02/15', 'yyyy/mm/dd')) DATA_CALC,
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
   SELECT TO_CHAR (REMI.N_ID_REMI) N_ID_REMI,
          ANAG_POOL.T_REMI REMI_POOL,
          TO_CHAR (ANAG_POOL.N_ID_REMI_ANAGRAFICA) N_ID_REMI_ANAGRAFICA,
          D.DATA_CALC,
          TAR.T_TARIFFA,
          Z.T_Z,
          PMAX.N_PMAX
     FROM INPUT_DATA D
          JOIN
          RCUGAS.RCUGAS_REMI REMI
             ON     1 = 1
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   REMI.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   REMI.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
          JOIN
          RCUGAS.RCUGAS_REMI_AGGREGAZIONE AGGR
             ON     AGGR.N_ID_REMI_ANAGRAFICA_FISICO =
                       REMI.N_ID_REMI_ANAGRAFICA
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   AGGR.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   AGGR.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
          JOIN
          RCUGAS.RCUGAS_REMI_ANAGRAFICA ANAG_POOL
             ON ANAG_POOL.N_ID_REMI_ANAGRAFICA =
                   AGGR.N_ID_REMI_ANAGRAFICA_POOL
          JOIN
          MADDARII.RCUGAS_REMI_PMAX PMAX
             ON     PMAX.N_ID_REMI_ANAGRAFICA =
                       ANAG_POOL.N_ID_REMI_ANAGRAFICA
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   PMAX.DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   PMAX.DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
          JOIN
          MADDARII.RCUGAS_REMI_TARIFFA TAR
             ON     TAR.N_ID_REMI_ANAGRAFICA = ANAG_POOL.N_ID_REMI_ANAGRAFICA
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   TAR.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   TAR.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
          JOIN
          MADDARII.RCUGAS_REMI_Z Z
             ON     Z.N_ID_REMI_ANAGRAFICA = ANAG_POOL.N_ID_REMI_ANAGRAFICA
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   Z.D_DATA_INIZIO,
                                                   (TO_DATE ('01/01/1900',
                                                             'DD/MM/YYYY')))
                                            AND NVL (
                                                   Z.D_DATA_FINE,
                                                   (TO_DATE ('31/12/2099',
                                                             'DD/MM/YYYY')))
    WHERE 1 = 1;