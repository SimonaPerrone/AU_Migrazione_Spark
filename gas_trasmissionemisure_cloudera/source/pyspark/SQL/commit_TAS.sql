INSERT INTO TABLE au.gas_tas 
SELECT  cod_servizio        ,
        cod_flusso          ,
        dataElaborazione    ,
        piva_utente         ,
        piva_distr          ,
        cod_pdr             ,
        matr_mis            ,
        matr_conv           ,
        data_com_autolet_cf ,
        let_tot_prel        ,
        let_tot_conv        ,
        esito_val           ,
        note 
from au_test.gas_tas