CREATE TABLE IF NOT EXISTS au_test.gas_profstand (
        data    STRING,
        c1b1    DOUBLE,
        c1c1    DOUBLE,
        c1d1    DOUBLE,
        c1e1    DOUBLE,
        c1f1    DOUBLE,
        c1b2    DOUBLE,
        c1c2    DOUBLE,
        c1d2    DOUBLE,
        c1e2    DOUBLE,
        c1f2    DOUBLE,
        c1b3    DOUBLE,
        c1c3    DOUBLE,
        c1d3    DOUBLE,
        c1e3    DOUBLE,
        c1f3    DOUBLE,
        c2      DOUBLE,
        c4      DOUBLE,
        t11     DOUBLE,
        t12     DOUBLE,
        t13     DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acutest/au/misure_gas_au/gas_profstand'