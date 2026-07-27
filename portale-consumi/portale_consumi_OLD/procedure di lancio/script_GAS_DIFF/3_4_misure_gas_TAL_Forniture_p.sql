drop table misuregas.letture_tal;
create table misuregas.letture_tal  Stored As Parquet as
select  PRT_CMG_TAL.cod_pdr,
    annomese_riferimento,
    dt_caricamento,
    let_tot_prel,
    data_lettura,
    codice_fornitura,
    'TAL' as Flusso
from CMG.PRT_CMG_TAL_o as PRT_CMG_TAL
inner join misuregas.forniture_gas on forniture_gas.t_codice_pdr=PRT_CMG_TAL.cod_pdr
where cast(date_format(data_lettura,'yyyyMMdd') as int) >=data_inizio_fornitura_num and
      cast(date_format(data_lettura,'yyyyMMdd') as int) <=data_fine_fornitura_num and
      NVL(forniture_gas.t_codice_pdr,'') <> '';
