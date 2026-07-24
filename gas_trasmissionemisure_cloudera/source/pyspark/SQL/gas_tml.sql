CREATE TABLE IF NOT EXISTS au_test.gas_tml (
        cod_servizio            String,
        cod_flusso              String,
        dataElaborazione        String,
        cod_pdr                 String,
        matr_mis                String,
        coeff_corr              Double,
        freq_let                String,
        acc_mis                 String,
        data_racc               timestamp,
        let_tot_prel            Double,
        let_tot_conv            Double,
        tipo_lettura            String,
        val_dato                String,
        num_tentativi           Double,
        esito_raccolta          String,
        mod_alt_racc            String,
        dir_indennizzo          String,
        pros_fin                String
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_tml'
