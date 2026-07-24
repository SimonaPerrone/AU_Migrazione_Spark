CREATE TABLE IF NOT EXISTS au_test.gas_ca_mensili (
        cod_pdr         String,
        ca              DOUBLE,
        dateStart       String,
        dateEnd         String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_ca_mensili'