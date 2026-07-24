CREATE TABLE IF NOT EXISTS au_test.gas_param_carat (
        PROF    String,
        B1      DOUBLE,
        B2      DOUBLE,
        B3      DOUBLE,
        B4      DOUBLE,
        Cat     String, 
        Zona    String,
        classe  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_param_carat'