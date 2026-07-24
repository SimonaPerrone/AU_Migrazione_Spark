INSERT INTO TABLE au.gas_tal_500 
SELECT  
        cod_flusso,
        dataElaborazione,
        piva_utente,
        piva_distr,
        cod_pdr,
        matr_mis,
        matr_conv,
        data_com_autolet_cf,
        let_tot_prel,
        esito_val,
        note
from au_test.gas_tal_500