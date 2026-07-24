--create table tmp_clg_perimetro_REMI as
WITH input_data AS (
  SELECT	trunc(to_date('15/02/2021','dd/mm/yyyy')) data_calc
            ,to_number(to_char(to_date('15/02/2021','dd/mm/yyyy'),'YYYYMMDDHH24MISS')) execution_id
            ,case
                when To_number(to_char(trunc(to_date('15/02/2021','dd/mm/yyyy')),'MM')) between 10 and 12 then to_char(add_months(trunc(to_date('15/02/2021','dd/mm/yyyy')), 12),'YYYY')
                else to_char(trunc(to_date('15/02/2021','dd/mm/yyyy')),'YYYY')
            end   anno
    from dual
    )  
SELECT  remi.N_ID_REMI
        ,ANAG_POOL.T_REMI REMI_POOL
        ,ANAG_POOL.N_ID_REMI_ANAGRAFICA
        ,d.data_calc      
        ,ROUND(DBMS_RANDOM.VALUE(1,100))        T_TARIFFA
        ,ROUND(DBMS_RANDOM.VALUE(0,1),1)        T_Z
        ,ROUND(DBMS_RANDOM.VALUE(0,2),2)        T_PMAX  
FROM  INPUT_DATA d                        
        join RCUGAS.RCUGAS_REMI REMI on 1 = 1 --REMI.N_ID_REMI=CON.N_ID_REMI
            AND TRUNC(d.data_calc) BETWEEN NVL(REMI.D_DATA_INIZIO, (TO_DATE('01/01/1900','DD/MM/YYYY'))) AND NVL(REMI.D_DATA_FINE, (TO_DATE('31/12/2099','DD/MM/YYYY')))
        join RCUGAS.RCUGAS_REMI_TIPO TIPO on TIPO.N_ID_REMI_ANAGRAFICA = REMI.N_ID_REMI_ANAGRAFICA --??
            AND TRUNC(d.data_calc) BETWEEN NVL(TIPO.D_DATA_INIZIO, (TO_DATE('01/01/1900','DD/MM/YYYY'))) AND NVL(TIPO.D_DATA_FINE, (TO_DATE('31/12/2099','DD/MM/YYYY')))                    
        join RCUGAS.RCUGAS_REMI_AGGREGAZIONE AGGR on AGGR.N_ID_REMI_ANAGRAFICA_FISICO=REMI.N_ID_REMI_ANAGRAFICA
            AND TRUNC(d.data_calc) BETWEEN NVL(AGGR.D_DATA_INIZIO, (TO_DATE('01/01/1900','DD/MM/YYYY'))) AND NVL(AGGR.D_DATA_FINE, (TO_DATE('31/12/2099','DD/MM/YYYY')))                    
        join RCUGAS.RCUGAS_REMI_ANAGRAFICA ANAG_POOL on ANAG_POOL.N_ID_REMI_ANAGRAFICA=AGGR.N_ID_REMI_ANAGRAFICA_POOL
        join RCUGAS.RCUGAS_REMI_TIPO TIPO_POOL on TIPO_POOL.N_ID_REMI_ANAGRAFICA=AGGR.N_ID_REMI_ANAGRAFICA_POOL --??
            AND TRUNC(d.data_calc) BETWEEN NVL(TIPO_POOL.D_DATA_INIZIO, (TO_DATE('01/01/1900','DD/MM/YYYY'))) AND NVL(TIPO_POOL.D_DATA_FINE, (TO_DATE('31/12/2099','DD/MM/YYYY')))                            
WHERE 1=1
--AND     PDR_STATO.T_COD_STATO_PDR = 'P'           
--AND     NVL(T_TRATTAMENTO_SETTLEMENT,'Y') IN ('G','M')
--AND     T_COD_CAT_USO IN ('T1','C2') 
--and rownum <= 1000
--and     n_id_remi = 150611000000003301