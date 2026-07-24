CREATE TABLE IF NOT EXISTS au_test.gas_def (
        codice_servizio	        string,	
        cod_flusso	        string,	
        data_deco_switch	string,	
        vol_annuo_sost	        string,	
        classe_gruppo_mis	string,	
        n_cifre_mis	        string,	
        matr_mis	        string,	
        cod_pdr	                string,	
        segn_mis_sost	        string,	
        tipo_lettura	        string,	
        pre_conv	        string,	
        gruppo_mis_int	        string,	
        coeff_corr	        string,	
        matr_conv	        string,	
        n_cifre_conv	        string,	
        segn_conv	        string,	
        data_mis_eff	        string,	
        segn_mis_eff	        string,	
        segn_conv_eff	        string,	
        note	                string
)
partitioned by (anno String, mese String, piva_distr String, piva_utente String ) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS PARQUET
LOCATION '/user/acu/au_test/misure_gas_au/gas_def'