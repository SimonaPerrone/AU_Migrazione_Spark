INSERT INTO TABLE au.gas_rmv 
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
        progr_anno_term  ,
        data_comp        ,
        let_tot_prel     ,
        let_tot_conv     ,
        mot_rett_lett   
from au_test.gas_rmv