drop table misuregas.letture_rml;
create table misuregas.letture_rml  Stored As Parquet as 
    select  cod_pdr,  
            annomese_riferimento, 
            dt_caricamento,
            let_tot_prel,
            data_racc,
            "E" as tipo_lettura,
            codice_fornitura,
            'RML' as Flusso,
            Motivazione
    from CMG.PRT_CMG_RML_o as PRT_CMG_RML
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_RML.cod_pdr  
  where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
	    cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and
        NVL(forniture_gas.t_codice_pdr,'') <> '';
