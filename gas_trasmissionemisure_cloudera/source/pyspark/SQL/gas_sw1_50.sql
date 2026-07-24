CREATE TABLE IF NOT EXISTS au_test.gas_sw1_50 (
        cod_servizio            String,
        cod_flusso              String,
        dataElaborazione        String,
        cod_prat_utente         String,
        cod_prat_distr          String,
        cod_pdr                 String,
        matr_mis                String,
        data_deco_switch        timestamp,
        vol_annuo_sost          Double,
        classe_gruppo_mis       String,
        n_cifre_mis             String,
        segn_mis_sost           String,
        tipo_lettura            String,
        pre_conv                String,
        gruppo_mis_int          String,
        coeff_corr              Double,
        data_mis_eff            timestamp,
        segn_mis_eff            String,
        matr_conv               String,
        n_cifre_conv            String,
        segn_conv               String,
        segn_conv_eff           String,
        note                    String
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_sw1_50'
