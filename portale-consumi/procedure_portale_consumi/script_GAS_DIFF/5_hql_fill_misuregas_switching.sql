--RIEPIMENTO MISURE MANCANTI NEI MESI DI INIZIO FORNITURA

DROP TABLE IF EXISTS forniture_gas_iniziali_senza_misure;

CREATE TABLE forniture_gas_iniziali_senza_misure
as
SELECT KK_KEY , t_codice_pdr,codice_fornitura,annomese_iniziof from
(
select CONCAT(FF.t_codice_pdr,FF.codice_fornitura) KK_KEY  , FF.t_codice_pdr,FF.codice_fornitura,
cast(substr(cast(FF.data_inizio_fornitura_num as string),1,6) as int) annomese_iniziof 
from misuregas.forniture_gas FF
) FO
left outer join misuregas.misure_gas_portale_af  M_MSAF
ON CONCAT(FO.KK_KEY,cast (FO.annomese_iniziof as string))=CONCAT(M_MSAF.cod_pdr,M_MSAF.n_id_fornitura,M_MSAF.competenza_consumi_af)
LEFT OUTER JOIN misuregas.misure_gas_portale_mensili M_MS
ON CONCAT(FO.KK_KEY,cast (FO.annomese_iniziof as string))=CONCAT(M_MS.cod_pdr,M_MS.n_id_fornitura,M_MS.competenza_consumi_mmmm)
WHERE M_MSAF.competenza_consumi_af IS NULL AND  M_MS.competenza_consumi_mmmm IS NULL ;


INSERT INTO misuregas.misure_gas_portale_mensili
SELECT t_codice_pdr cod_pdr  ,codice_fornitura n_id_fornitura ,
annomese_iniziof competenza_consumi_mmmm ,'' tipo_misura_mmmm,
null data_lettura_mmmm, null lettura_misure_mmmm ,
null delta_misure_mmmm
from forniture_gas_iniziali_senza_misure;

INSERT INTO misuregas.misure_gas_portale_af
SELECT t_codice_pdr cod_pdr  , annomese_iniziof competenza_consumi_af,
null data_lettura_af,
'' tipo_misura_af,codice_fornitura n_id_fornitura ,
null lettura_mese_af , null delta_misure_af
from forniture_gas_iniziali_senza_misure;

DROP TABLE IF EXISTS forniture_gas_iniziali_senza_misure;

--MISURE ANTE SWITCHING 

DROP TABLE IF EXISTS fornituregas_switching;
CREATE TABLE fornituregas_switching
as
select CONCAT(FF.t_codice_pdr,FF.codice_fornitura) KK_KEY  , FF.data_fine_fornitura,
cast(substr(cast(FF.data_fine_fornitura_num as string),1,6) as int)annomese_finef ,
MAX(cast(substr(cast(FF.data_fine_fornitura_num as string),1,6) as int))  over ( partition by FF.t_codice_fiscale,FF.t_codice_pdr) max_annomese_fornitura
from misuregas.forniture_gas FF
WHERE CONCAT(FF.t_codice_fiscale,FF.t_codice_pdr) IN(SELECT CONCAT(FF_2.t_codice_fiscale,FF_2.t_codice_pdr) 
from misuregas.forniture_gas FF_2  group by FF_2.t_codice_fiscale,FF_2.t_codice_pdr having count(*) > 1);


INSERT INTO misuregas.misure_gas_portale_mensili  
SELECT cod_pdr  ,n_id_fornitura ,annomese_finef competenza_consumi_mmmm ,CONCAT('SW_',tipo_misura_mmmm) tipo_misura_mmmm,
data_fine_fornitura data_lettura_mmmm, null lettura_misure_mmmm ,
null delta_misure_mmmm
from 
(
select * 
from 
(SELECT *, CONCAT(cod_pdr,n_id_fornitura)KK_MS,MAX(competenza_consumi_mmmm) 
 over ( partition by cod_pdr,n_id_fornitura) max_misura_fornitura from misuregas.misure_gas_portale_mensili) MS
inner join fornituregas_switching  forniture
ON KK_MS = KK_KEY
WHERE max_misura_fornitura <> annomese_finef
and annomese_finef <> max_annomese_fornitura
)TF where max_misura_fornitura = competenza_consumi_mmmm;


INSERT INTO misuregas.misure_gas_portale_af 
SELECT cod_pdr  , annomese_finef competenza_consumi_af,data_fine_fornitura data_lettura_af,
CONCAT('SW_',tipo_misura_af) tipo_misura_af,n_id_fornitura ,
null lettura_mese_af , null delta_misure_af
from 
(
select * 
from 
(SELECT *, CONCAT(cod_pdr,n_id_fornitura)KK_MS,MAX(competenza_consumi_af) 
 over ( partition by cod_pdr,n_id_fornitura) max_misura_fornitura from misuregas.misure_gas_portale_af) MS
inner join fornituregas_switching  forniture
ON KK_MS = KK_KEY
WHERE max_misura_fornitura <> annomese_finef
and annomese_finef <> max_annomese_fornitura
)TF where max_misura_fornitura = competenza_consumi_af;

DROP TABLE IF EXISTS fornituregas_switching;
SET hive.auto.convert.join=true;

MSCK REPAIR TABLE misuregas.misure_gas_portale_mensili;
MSCK REPAIR TABLE misuregas.misure_gas_portale_af;



