CREATE TABLE IF NOT EXISTS au_test.gas_tas (
        cod_servizio            String,
        cod_flusso              String,
        dataElaborazione        String,
        cod_pdr                 String,
        matr_mis                String,
        matr_conv               String,
        data_com_autolet_cf     timestamp,
        let_tot_prel            Double,
        let_tot_conv            String,
        esito_val               String,
        note                    String
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_tas'
