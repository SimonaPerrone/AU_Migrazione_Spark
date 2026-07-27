MAPREDUCEPARAM-FILTER

DROP TABLE IF EXISTS ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb;
CREATE TABLE ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.bb AS
SELECT ANNOMESE_SW,pod14,PIVA_DISTR,PIVA_UDD,TRATTAMENTO_ONLINE,t_cod_contr_disp as DP,tipo_misuratore_last,d_data_decorrenza,d_creazione  FROM (VIEWSWCONTRAVIEWRSPURGED-FILTER) X
group by ANNOMESE_SW,pod14,PIVA_DISTR,PIVA_UDD,TRATTAMENTO_ONLINE,t_cod_contr_disp,tipo_misuratore_last,d_data_decorrenza,d_creazione;
