DROP TABLE misuregas.PRT_SWG;
CREATE TABLE misuregas.PRT_SWG stored as parquet as         
SELECT
PRT_SWG.t_codice_pdr,
MIN(D_DATA_DECORRENZA) AS D_DATA_DECORRENZA
FROM (select * from SWITCH_GAS.PRT_SWG_p as PRT_SWG where  T_STATO not in ( 'B','TE2','TE3','E1','E2','E3'))PRT_SWG
WHERE D_DATA_DECORRENZA>= from_unixtime(unix_timestamp())
GROUP BY t_codice_pdr;
   
   