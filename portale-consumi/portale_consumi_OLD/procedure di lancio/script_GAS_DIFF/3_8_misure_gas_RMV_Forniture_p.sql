drop table misuregas.letture_rmv;
create table misuregas.letture_rmv  Stored As Parquet as 
  select  cod_pdr,
            annomese_riferimento,
            dt_caricamento,
            let_tot_prel,
            data_comp,
            tipo_lettura,
            codice_fornitura,
            'RMV' as Flusso,
            Motivazione            
    from CMG.PRT_CMG_RMV_o as PRT_CMG_RMV
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RMV.cod_pdr
    where cast(date_format(data_comp,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_comp,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
