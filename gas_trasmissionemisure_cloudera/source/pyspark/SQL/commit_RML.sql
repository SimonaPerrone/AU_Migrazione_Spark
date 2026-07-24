INSERT INTO TABLE au.gas_rml_55
SELECT 
        cod_servizio     ,
        cod_flusso       ,
        dataElaborazione ,
        piva_utente      ,
        piva_distr       ,
        cod_pdr          ,
        matr_mis         ,
        matr_conv        ,
        coeff_corr       ,
        freq_let         ,
        data_comp        ,
        data_racc        ,
        let_tot_prel     ,
        let_tot_conv     ,
        mot_rett_lett    
from au_test.gas_rml_55