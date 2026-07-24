CREATE TABLE IF NOT EXISTS au_test.gas_rgl_55 (
        cod_servizio     String,
        cod_flusso       String,
        dataElaborazione String,
        mese_comp        String,
        cod_pdr          String,
        matr_mis         String,
        matr_conv        String,
        data_racc        timestamp,
        let_tot_prel     Double,
        let_tot_conv     String,
        mot_rett_lett    String,
        vol_ric          String,
        periodo_ric      String
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_rgl_55'
