CREATE TABLE IF NOT EXISTS au_test.gas_error_parsed (
        fileXML     String,
        fileXSD     String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_error_parsed'