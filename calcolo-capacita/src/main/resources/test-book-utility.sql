CHECK IMPORT :
ORACLE:
select count(distinct n_id_remi), count(*) from CLG.CLG_PERIMETRO_REMI_GM_VIEW
-----------------------------------------------
| COUNT(DISTINCTN_ID_REMI) | COUNT(*)         |
-----------------------------------------------
| 297                  | 12218                |
-----------------------------------------------

select count(distinct n_id_remi), count(*) from CLG.CLG_PERIMETRO_PDR_GM_VIEW

-----------------------------------------------
| COUNT(DISTINCTN_ID_REMI) | COUNT(*)         |
-----------------------------------------------
| 6                    | 7                    |
-----------------------------------------------

select count(distinct t_codice_pdr), count(*) from CLG.CLG_PERIMETRO_PDR_GM_VIEW
-----------------------------------------------
| COUNT(DISTINCTT_CODICE_PDR) | COUNT(*)             |
-----------------------------------------------
| 7                    | 7                    |
-----------------------------------------------
select count(distinct t_codice_pdr), count(*) from CLG.CLG_CONFIG_ESECUZIONI_PUNTUALI

-----------------------------------------------
| COUNT(DISTINCTT_CODICE_PDR) | COUNT(*)      |
-----------------------------------------------
| 51                   | 51                   |
-----------------------------------------------

HIVE:
select count(distinct n_id_remi), count(*) from eng_test.CLG_PERIMETRO_REMI_GM_VIEW_TMP
297	12218
select count(distinct n_id_remi), count(*) from eng_test.CLG_PERIMETRO_PDR_GM_VIEW_TMP
6	7
select count(distinct t_codice_pdr), count(*) from eng_test.CLG_PERIMETRO_PDR_GM_VIEW_TMP
7	7
select count(distinct t_codice_pdr), count(*) from eng_test.CLG_CONFIG_ESECUZIONI_PUNTUALI_TMP
51	51
select count(distinct n_id_remi), count(*) from eng_test.clg_anagrafica_gm_view
5	246
select count(distinct t_codice_pdr), count(*) from eng_test.clg_anagrafica_gm_view
6	246
--CHECK EXPORT
ORACLE:
select count(distinct t_codice_pdr), count(*) from CLG.CLG_PDR_CAPACITA_TMP where n_execution_id=20210402145651;
-----------------------------------------------
| COUNT(DISTINCTT_CODICE_PDR) | COUNT(*)             |
-----------------------------------------------
| 27375                | 27375                |
-----------------------------------------------

HIVE:
select count(distinct t_codice_pdr), count(*) from eng_test.CLG_PDR_CAPACITA_TMP where n_execution_id=20210402145651;
	27375	27375
--CHECK_MASSIVO
--HIVE:
--##################################
-- controllo duplicati
select count(distinct t_codice_pdr)=count(*) from eng_test.CLG_PDR_CAPACITA_TMP;
--##################################
-- controllo dimensione output del risultato
SET mapreduce.map.memory.mb=70000;
SET mapreduce.map.java.opts.max.heap=50000;
SET mapreduce.map.java.opts=-Xmx50000m;
SET mapreduce.reduce.memory.mb=70000;
SET mapreduce.reduce.java.opts=-Xmx50000m;
SET mapreduce.reduce.java.opts.max.heap=50000;
WITH num_pdr_anagrafica AS
  (SELECT count(*) AS count_anagrafica
   FROM eng_test.clg_anagrafica_gm_view),
     num_pdr_filtered AS
  (SELECT count(*) AS count_pdr_filtered
   FROM eng_test.clg_config_esecuzioni_puntuali_tmp
   WHERE t_valido='S' ),
     num_pdr_result AS
  (SELECT count(*) AS count_pdr_res
   FROM eng_test.clg_pdr_capacita_tmp)
SELECT CASE
           WHEN p.count_pdr_filtered = 0 THEN (r.count_pdr_res = n.count_anagrafica)
           ELSE (r.count_pdr_res = p.count_pdr_filtered)
       END
FROM num_pdr_result r
LEFT JOIN num_pdr_anagrafica n
LEFT JOIN num_pdr_filtered p;
--##################################
--controllo valori null sul result
SELECT count(*)=0
FROM eng_test.clg_pdr_capacita_tmp
where
n_pcm is null or
n_cl is null or
n_pcm is null or
n_execution_id is null or
t_tipo_calcolo is null;
--##################################
-- controllo valori per tipo di calcolo
WITH pos_value AS
  (SELECT distinct(t_tipo_calcolo) tipo_calcolo
   FROM eng_test.clg_pdr_capacita_tmp)
SELECT CASE
           WHEN pos_value.tipo_calcolo IN ('tipo_calc_1',
                                           'tipo_calc_2',
                                           'tipo_calc_3') THEN true
           ELSE false
       END
FROM pos_value;
--non deve tornare false
--################################
SET mapreduce.map.memory.mb=70000;
SET mapreduce.map.java.opts.max.heap=50000;
SET mapreduce.map.java.opts=-Xmx50000m;
SET mapreduce.reduce.memory.mb=70000;
SET mapreduce.reduce.java.opts=-Xmx50000m;
SET mapreduce.reduce.java.opts.max.heap=50000;

set cod_pdr=00080000210604;

with union_m as(
select cod_pdr,giorno,consumo,202102 as mese from sbg_202102.calcolo_consumi_y_g_cons_mas_t_t_t_t
where cod_pdr=${hiveconf:cod_pdr}
union all
select cod_pdr,giorno,consumo,202101 as mese from sbg_202101.calcolo_consumi_y_g_cons_mas_t_t_t_t
where cod_pdr=${hiveconf:cod_pdr}
union all
select cod_pdr,giorno,consumo,202012 as mese from sbg_202012.calcolo_consumi_y_g_cons_mas_t_t_t_t
where cod_pdr=${hiveconf:cod_pdr}
)
select cod_pdr,giorno,mese,consumo from union_m
order by cod_pdr,mese desc, giorno desc;


DIFFERENZA TRA STESSA TABELLA
select id_pdr_capacita_tmp, n_id_pdr , n_id_pratica ,t_codice_pdr ,d_data_calcolo ,n_anno ,n_mese ,t_tipo_calcolo ,n_pcm ,n_ctc ,n_cl ,d_data_inizio ,d_data_fine ,t_origine ,t_processo  ,t_esito_agg_rcu ,t_errore_agg_rcu ,t_esito_agg_rcu_desc ,t_stato ,d_data_aggiornamento ,count(*)
from
(select id_pdr_capacita_tmp, n_id_pdr , n_id_pratica ,t_codice_pdr ,d_data_calcolo ,n_anno ,n_mese ,t_tipo_calcolo ,n_pcm ,n_ctc ,n_cl ,d_data_inizio ,d_data_fine ,t_origine ,t_processo  ,t_esito_agg_rcu ,t_errore_agg_rcu ,t_esito_agg_rcu_desc ,t_stato ,d_data_aggiornamento
from eng_test.clg_pdr_capacita_tmp_bck
union all
select id_pdr_capacita_tmp, n_id_pdr , n_id_pratica ,t_codice_pdr ,d_data_calcolo ,n_anno ,n_mese ,t_tipo_calcolo ,n_pcm ,n_ctc ,n_cl ,d_data_inizio ,d_data_fine ,t_origine ,t_processo  ,t_esito_agg_rcu ,t_errore_agg_rcu ,t_esito_agg_rcu_desc ,t_stato ,d_data_aggiornamento
from eng_test.clg_pdr_capacita_tmp) t
group by t.id_pdr_capacita_tmp, t.n_id_pdr , t.n_id_pratica , t.t_codice_pdr , t.d_data_calcolo , t.n_anno , t.n_mese , t.t_tipo_calcolo , t.n_pcm , t.n_ctc , t.n_cl , t.d_data_inizio , t.d_data_fine , t.t_origine , t.t_processo  , t.t_esito_agg_rcu , t.t_errore_agg_rcu , t.t_esito_agg_rcu_desc , t.t_stato , t.d_data_aggiornamento
having count(*)<>2