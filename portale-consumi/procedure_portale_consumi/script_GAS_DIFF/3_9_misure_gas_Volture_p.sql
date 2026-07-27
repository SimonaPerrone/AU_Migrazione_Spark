----Volture
Drop Table misuregas.VoltureGas;
Create Table misuregas.VoltureGas Stored as Parquet as
select *
From
(
SELECT distinct PRT_VTG6.cod_pdr,
PRT_VTG6.annomese_riferimento as competenza_consumi_v,
PRT_VTG6.dt_caricamento  as data_lettura_v,
PRT_VTG6.tipo_flusso as tipo_misure_v,
PRT_VTG6.codice_fornitura as codice_fornitura_v,
PRT_VTG6.let_tot_prel     as lettura_v
FROM misuregas.misure_storic PRT_VTG6 where tipo_flusso='VTG6'
and PRT_VTG6.cod_pdr_rmv is null
) as V
union all select * from
(
select  cod_pdr,
annomese_riferimento as competenza_consumi_v,
dt_caricamento  as data_lettura_v,
tipo_flusso as tipo_misure_v,
codice_fornitura as codice_fornitura_v,
let_tot_prel     as lettura_v
from   misuregas.misure_storic where tipo_flusso='RMV'
) as RMV;