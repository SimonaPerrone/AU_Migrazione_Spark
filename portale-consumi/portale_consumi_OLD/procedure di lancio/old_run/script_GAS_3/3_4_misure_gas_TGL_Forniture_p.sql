drop table misuregas.letture_tgl;
create table misuregas.letture_tgl Stored As Parquet as 
    select  cod_pdr,  
            mese_comp, 
            dt_caricamento,
            let_tot_prel,
            data_comp,
            tipo_lettura,
            codice_fornitura,
            'TGL' as Flusso,
            '' as Motivazione
    from CMG.PRT_CMG_TGL_o as PRT_CMG_TGL
     inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TGL.cod_pdr
    where cast(date_format(data_comp,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_comp,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
