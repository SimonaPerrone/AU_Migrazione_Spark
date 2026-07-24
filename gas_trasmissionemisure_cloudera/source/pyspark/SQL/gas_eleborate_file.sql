CREATE TABLE IF NOT EXISTS au_test.gas_eleborate_file (
        namefile                String,
        esito                   String,
        dataElaborazione        String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_eleborate_file'
