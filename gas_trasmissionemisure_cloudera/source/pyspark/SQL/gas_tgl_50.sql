CREATE TABLE IF NOT EXISTS au_test.gas_tgl_50 (
        cod_servizio     String,
        cod_flusso       String,
        dataElaborazione String,
        mese_comp        String,
        cod_pdr          String,
        matr_mis         String,
        val_dato_mens    String,
        esito_raccolta   String,
        data_comp        timestamp,
        let_tot_prel     Double,
        tipo_lettura     String
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_tgl_50'
