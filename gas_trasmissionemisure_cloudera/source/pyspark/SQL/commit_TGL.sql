INSERT INTO TABLE au.gas_tgl_50 
SELECT  cod_servizio    ,
        cod_flusso      ,
        dataElaborazione,
        piva_utente     ,
        piva_distr      ,
        mese_comp       ,
        cod_pdr         ,
        matr_mis        ,
        val_dato_mens   ,
        esito_raccolta  ,
        data_comp       ,
        let_tot_prel    ,
        tipo_lettura 
from au_test.gas_tgl_50