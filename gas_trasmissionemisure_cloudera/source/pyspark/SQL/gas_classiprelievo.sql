CREATE TABLE IF NOT EXISTS au_test.gas_classiprelievo (
        Codice           String,
        Descrizione      String,
        Giorni           INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_classiprelievo'