INSERT INTO TABLE au.gas_rgl_55 
SELECT  cod_servizio    , 
        cod_flusso      , 
        dataElaborazione, 
        piva_utente     , 
        piva_distr      , 
        mese_comp       , 
        cod_pdr         , 
        matr_mis        , 
        matr_conv       , 
        data_racc       , 
        let_tot_prel    , 
        let_tot_conv    , 
        mot_rett_lett   , 
        vol_ric         , 
        periodo_ric      
from au_test.gas_rgl_55