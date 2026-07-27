drop table misuregas.letture_tav;
create table misuregas.letture_tav Stored As Parquet as
select  PRT_CMG_TAV.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAV' as Flusso
from CMG.PRT_CMG_TAV_o as PRT_CMG_TAV
inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAV.cod_pdr
where cast(date_format(data_lettura,'yyyyMMdd') as int) >=data_inizio_fornitura_num and
      cast(date_format(data_lettura,'yyyyMMdd') as int) <=data_fine_fornitura_num and
      NVL(forniture_gas.t_codice_pdr,'') <> '';
