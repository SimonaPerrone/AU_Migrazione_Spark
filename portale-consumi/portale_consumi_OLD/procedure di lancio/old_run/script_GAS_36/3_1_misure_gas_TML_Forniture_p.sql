drop table misuregas.letture_tml;
create table misuregas.letture_tml  Stored As Parquet as 
  select  cod_pdr,
            annomese_riferimento,
            dt_caricamento,
            let_tot_prel,
            data_racc,
            tipo_lettura,
            codice_fornitura,
            'TML' as Flusso            
    from CMG.PRT_CMG_TML_o as PRT_CMG_TML
    inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TML.cod_pdr
    where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
	      cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
		  NVL(forniture_gas.t_codice_pdr,'') <> '';


        
