CREATE TABLE IF NOT EXISTS au_test.gas_categtermica (
        Codice           String,
        Descrizione      String,
        CompTermica      BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_categtermica'