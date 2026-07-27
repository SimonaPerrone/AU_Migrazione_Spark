drop table misuregas.letture_vtg;
Create Table misuregas.letture_vtg Stored AS Parquet as 
SELECT distinct PRT_VTG6.cod_pdr,
 PRT_VTG6.annomese_riferimento, 
             PRT_VTG6.dt_caricamento,
            PRT_VTG6.let_tot_prel,
            PRT_VTG6.data_racc,
            PRT_VTG6.tipo_lettura,
            codice_fornitura,
            "VTG6" as flusso,
	    PRT_CMG_RMV.cod_pdr cod_pdr_rmv
            FROM SWITCH_GAS.PRT_VTG6_o as PRT_VTG6
            inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_VTG6.cod_pdr  
	    left outer join  CMG.PRT_CMG_RMV_o as PRT_CMG_RMV on  PRT_VTG6.cod_pdr=PRT_CMG_RMV.cod_pdr and PRT_VTG6.annomese_riferimento=PRT_CMG_RMV.annomese_riferimento
    where cast(date_format(data_racc,'yyyyMMdd') as int) >=data_inizio_fornitura_num and 
          cast(date_format(data_racc,'yyyyMMdd') as int) <=data_fine_fornitura_num and 
          NVL(forniture_gas.t_codice_pdr,'') <> '';
