DROP VIEW CLG.CLG_PERIMETRO_REMI_V_COLL;

/* Formatted on 14/10/2021 15:41:11 (QP5 v5.227.12220.39754) */
CREATE OR REPLACE FORCE VIEW CLG.CLG_PERIMETRO_REMI_V_COLL
(
   DATA_CALC,
   ANNO,
   N_ID_REMI,
   T_REMI,
   N_ID_REMI_ANAGRAFICA,
   T_TIPO_REMI,
   T_Z,
   T_PMAX
)
AS
   WITH INPUT_DATA
        AS (SELECT TRUNC (TO_DATE ('2020/08/30', 'yyyy/mm/dd')) DATA_CALC,
                   CASE
                      WHEN TO_NUMBER (
                              TO_CHAR (
                                 TRUNC (TO_DATE ('2020/08/30', 'yyyy/mm/dd')),
                                 'MM')) BETWEEN 10
                                            AND 12
                      THEN
                         TO_CHAR (
                            ADD_MONTHS (
                               TRUNC (TO_DATE ('2020/08/30', 'yyyy/mm/dd')),
                               12),
                            'YYYY')
                      ELSE
                         TO_CHAR (
                            TRUNC (TO_DATE ('2020/08/30', 'yyyy/mm/dd')),
                            'YYYY')
                   END
                      ANNO
              FROM DUAL)
   SELECT DISTINCT
          D.DATA_CALC,
          D.ANNO,
          TO_CHAR (REMI.N_ID_REMI) N_ID_REMI,
          ANAG_POOL.T_REMI,
          TO_CHAR (ANAG_POOL.N_ID_REMI_ANAGRAFICA) N_ID_REMI_ANAGRAFICA,
          TIPO.T_TIPO_REMI                                --            ,Z.T_Z
                                                    --            ,PMAX.N_PMAX
          ,
          ROUND (DBMS_RANDOM.VALUE (0, 100), 2) N_Z,
          ROUND (DBMS_RANDOM.VALUE (0, 100), 2) N_PMAX
     FROM INPUT_DATA D
          JOIN
          RCUGAS.RCUGAS_REMI REMI
             ON     1 = 1
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   REMI.D_DATA_INIZIO,
                                                   (TO_DATE ('1900', 'YYYY')))
                                            AND NVL (
                                                   REMI.D_DATA_FINE,
                                                   (TO_DATE ('9999', 'YYYY')))
          JOIN
          RCUGAS.RCUGAS_REMI_AGGREGAZIONE AGGR
             ON     AGGR.N_ID_REMI_ANAGRAFICA_FISICO =
                       REMI.N_ID_REMI_ANAGRAFICA
                AND TRUNC (D.DATA_CALC) BETWEEN NVL (
                                                   AGGR.D_DATA_INIZIO,
                                                   (TO_DATE ('1900', 'YYYY')))
                                            AND NVL (
                                                   AGGR.D_DATA_FINE,
                                                   (TO_DATE ('9999', 'YYYY')))
          JOIN
          RCUGAS.RCUGAS_REMI_ANAGRAFICA ANAG_POOL
             ON ANAG_POOL.N_ID_REMI_ANAGRAFICA =
                   AGGR.N_ID_REMI_ANAGRAFICA_POOL
          JOIN
          RCUGAS.RCUGAS_REMI_TIPO TIPO
             ON     TIPO.N_ID_REMI_ANAGRAFICA =
                       ANAG_POOL.N_ID_REMI_ANAGRAFICA
                AND TIPO.T_TIPO_REMI IN ('P', 'S')
    --       JOIN clg.RCUGAS_REMI_PMAX PMAX ON PMAX.N_ID_REMI_ANAGRAFICA = ANAG_POOL.N_ID_REMI_ANAGRAFICA
    --            AND TRUNC (D.DATA_CALC) BETWEEN NVL ( PMAX.DATA_INIZIO, (TO_DATE ('1900','YYYY'))) AND NVL ( PMAX.DATA_FINE,(TO_DATE ('9999','YYYY')))
    --          JOIN rcugas.RCUGAS_REMI_Z Z ON     Z.N_ID_REMI_ANAGRAFICA = ANAG_POOL.N_ID_REMI_ANAGRAFICA
    --            AND TRUNC (D.DATA_CALC) BETWEEN NVL (Z.D_DATA_INIZIO, (TO_DATE ('1900','YYYY'))) AND NVL (Z.D_DATA_FINE, (TO_DATE ('9999','YYYY')))
    WHERE 1 = 1;
