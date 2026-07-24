---- TABELLA DI PRODUZIONE ----

-- COD_REMI_ID_REG_CLIM
CREATE EXTERNAL TABLE IF NOT EXISTS au.COD_REMI_ID_REG_CLIM 
(
    T_COD_REMI String, 
    ID_REG_CLIM String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/acquirente_unico/cod_remi_id_reg_cli';

-- TAB_FATT_CLIM_WK
CREATE EXTERNAL TABLE IF NOT EXISTS au.TAB_FATT_TFC
(
    Data string,
    ID_REG_CLIM int,
    WKR double
)    
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE location '/user/hive/warehouse/settle_gas.db/TAB_FATT_TFC' 
TBLPROPERTIES("skip.header.line.count"="1");

-- C1
CREATE EXTERNAL TABLE IF NOT EXISTS au.TAB_FATT_C1
(
    Data string,
    C1 double,
    KeyZonaClasse string,
    KeyDataClasse string
)    
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','                     
STORED AS TEXTFILE location '/user/hive/warehouse/settle_gas.db/TAB_FATT_C1' 
TBLPROPERTIES("skip.header.line.count"="1");  

-- T1
CREATE EXTERNAL TABLE IF NOT EXISTS au.TAB_FATT_T1
(
    Data string,
    t1 double,
    KeyDataClasse string
)    
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                     
STORED AS TEXTFILE location '/user/hive/warehouse/settle_gas.db/TAB_FATT_T1' 
TBLPROPERTIES("skip.header.line.count"="1");


-- TAB_FATT_C2C4
CREATE EXTERNAL TABLE IF NOT EXISTS au.TAB_FATT_C2C4
(
    Data string,
    c2 double,
    c4 double
)    
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                     
STORED AS TEXTFILE location '/user/hive/warehouse/settle_gas.db/TAB_FATT_C2C4' 
TBLPROPERTIES("skip.header.line.count"="1");  


-- ELENCO_CODICE_ISTAT_MANC
CREATE EXTERNAL TABLE IF NOT EXISTS sferrara.ELENCO_CODICE_ISTAT_MANC 
(
   T_CODICE_PDR String, 
   T_CODICE_ISTAT String
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/user/silvia/sferrara/elenco_codice_istat_manc'
TBLPROPERTIES("skip.header.line.count"="1"); 

-- ParametriPProf
-- CREATE EXTERNAL TABLE IF NOT EXISTS au.ParametriPProf
-- (
--     DATA datetime,
--     WKR double,
--     PROF string,
--     C1 double,
--     c2 double,
--     c4 double,
--     t1 double,
--     ID_REG_CLIM string
-- )    
-- ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                     
-- STORED AS TEXTFILE location '/user/hive/warehouse/settle_gas.db/ParametriPProf'   
-- TBLPROPERTIES("skip.header.line.count"="1");


-- TAB_PARAMETRI_CARATTERISTICI_PROF_PREL
CREATE EXTERNAL TABLE IF NOT EXISTS au.TAB_PARAMETRI_CARATTERISTICI_PROF_PREL(
  prof string,
  b1 double,
  b2 double,
  b3 double,
  b4 double,
  cat_uso string,
  zona_clim string,
  class_prelievo double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'                     
STORED AS TEXTFILE location '/user/acutest/au/misure_gas_au/TAB_PARAMETRI_CARATTERISTICI_PROF_PREL'   
TBLPROPERTIES("skip.header.line.count"="1");

