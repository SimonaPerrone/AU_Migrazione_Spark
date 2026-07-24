CREATE TABLE IF NOT EXISTS au_test.gas_wk (
        data    STRING,
        Zona    String,
        valore  DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_wk'