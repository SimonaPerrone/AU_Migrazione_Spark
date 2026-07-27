Select max(executionid)
from default.ca;

Select *
from default.ca
where executionid=${executionid} and idcaerrorcode=0 and pdr='${pdr}'
;

SELECT *
from settle_gas.gas_tds
where valid=true
and cod_pdr='${pdr}'
;

SELECT *
from prt.istat_regione_climatica_p
where t_codice_istat='123069';

drop table default.ca_final;
create table default.ca_final as
# dopo calcolo ca per creare la ca final (logica implementata Pdr massivo Controller)
SELECT * from (
SELECT
row_number() over (order by 1) as ID_SAG_ANN ,
"2021" as ANNO_COMPETENZA,
cast(rcu_dist.n_id_distr as BIGINT) as N_ID_DISTR,
CAST(ca_p.n_id_az_udd as BIGINT) as N_ID_AZ_UDD,
CAST(rcu_bp.n_id_udb as BIGINT) as N_ID_UDB,
rcu_dist.t_remi as CODICE_REMI,
ca_p.t_codice_pdr as CODICE_PDR,
CAST(NULL as STRING) as CAP_TRASP_PDR,
substr(CASE WHEN ca.next_cod_profilo is not null AND ca.next_cod_profilo <> "" THEN ca.next_cod_profilo ELSE last_cod_prof.t_cod_profilo END, 1, 2) as CAT_USO,
substr(CASE WHEN ca.next_cod_profilo is not null AND ca.next_cod_profilo <> "" THEN ca.next_cod_profilo ELSE last_cod_prof.t_cod_profilo END, 4, 1) as CLASSE_PRELIEVO,
CASE WHEN substr(CASE WHEN ca.next_cod_profilo is not null AND ca.next_cod_profilo <> "" THEN ca.next_cod_profilo ELSE last_cod_prof.t_cod_profilo END, 3, 1) = "X"
THEN zona_clim.t_regione_climatica ELSE
substr(CASE WHEN ca.next_cod_profilo is not null AND ca.next_cod_profilo <> "" THEN ca.next_cod_profilo ELSE last_cod_prof.t_cod_profilo END, 3, 1)
END as ZONA_CLIMATICA,
CASE WHEN ca.id_regclim is not null AND ca.id_regclim > 0 THEN CAST(ca.id_regclim as string) ELSE ca_p.id_regione_climatica END as ID_REG_CLIM,
CASE WHEN ca.next_cod_profilo is not null AND ca.next_cod_profilo <> "" THEN ca.next_cod_profilo ELSE last_cod_prof.t_cod_profilo END as COD_PROF_PREL_STD,
CAST(round(CASE WHEN ca.ca is not null THEN ca.ca ELSE CAST(last_ca.n_prelievo_annuo as DOUBLE) END) as STRING) as PRELIEVO_ANNUO_PREV,
ca_p.t_trattamento TRATTAMENTO,
from_unixtime(unix_timestamp(), "dd/MM/yyyy") as D_RICEZIONE,
"PRE" as TIPO_TRASMISSIONE,
ca.t_comune_istat_pdr as codIstat

from (
	SELECT *
	from rcugas.rcugas_massivo_ca_p
	where from_unixtime(unix_timestamp('2020-07-01','yyyy-MM-dd'))
		  between from_unixtime(unix_timestamp(nvl(d_data_inizio_for,'1900-01-01'),'yyyy-MM-dd'))
		  and from_unixtime(unix_timestamp(nvl(data_fine_for,'2900-01-01'),'yyyy-MM-dd'))
) ca_p

INNER JOIN (
	SELECT *
	FROM rcugas.rcugas_connessioni_distr2_p
	where from_unixtime(unix_timestamp('2020-07-01','yyyy-MM-dd'))
		between from_unixtime(unix_timestamp(nvl(d_data_inizio_conn,'1900-01-01'),'yyyy-MM-dd'))
		and from_unixtime(unix_timestamp(nvl(d_data_fine_conn,'2900-01-01'),'yyyy-MM-dd'))
) rcu_dist on rcu_dist.t_codice_pdr=ca_p.t_codice_pdr

LEFT JOIN (
  SELECT *
  from rcugas.rcugas_bilanciamento_p
  where from_unixtime(unix_timestamp('2020-07-01','yyyy-MM-dd'))
        between from_unixtime(unix_timestamp(nvl(d_data_inizio,'1900-01-01'),'yyyy-MM-dd'))
        and from_unixtime(unix_timestamp(nvl(d_data_fine,'2900-01-01'),'yyyy-MM-dd'))
) rcu_bp on rcu_bp.n_id_pdr=rcu_dist.n_id_pdr

INNER JOIN (
	SELECT t_codice_pdr, t_cod_profilo from (
		SELECT t_codice_pdr, t_cod_profilo, row_number() over (partition by t_codice_pdr order by from_unixtime(unix_timestamp(nvl(d_data_inizio_for,'1900-01-01'),'yyyy-MM-dd')) desc) as prio
		from rcugas.rcugas_massivo_ca_p
		where t_cod_profilo is NOT NULL and t_cod_profilo <> ""
	) as lastcod
	where prio=1
) last_cod_prof on last_cod_prof.t_codice_pdr=ca_p.t_codice_pdr

INNER JOIN (
	SELECT t_codice_pdr, n_prelievo_annuo from (
		SELECT t_codice_pdr, n_prelievo_annuo, row_number() over (partition by t_codice_pdr order by from_unixtime(unix_timestamp(nvl(d_data_inizio_for,'1900-01-01'),'yyyy-MM-dd')) desc) as prio
		from rcugas.rcugas_massivo_ca_p
		where n_prelievo_annuo is NOT NULL and n_prelievo_annuo <> ""
	) last_pre
	where prio=1
) last_ca on last_ca.t_codice_pdr=ca_p.t_codice_pdr
left join (
    SELECT DISTINCT pdr, next_cod_profilo, id_regclim, t_comune_istat_pdr, ca_sum as ca
    from default.ca
    where executionid=${executionid} and idcaerrorcode=0
) as ca on ca_p.t_codice_pdr = ca.pdr
LEFT JOIN (
    SELECT distinct t_codice_pdr, t_comune_istat_pdr, t_regione_climatica
    from (
        SELECT *, rank() over(PARTITION BY t_codice_pdr ORDER BY t_anno_termico desc) rank
        from rcugas.rcugas_massivo_ca_p
    ) cod_is
    INNER JOIN prt.istat_regione_climatica_p z  on cod_is.t_comune_istat_pdr = z.t_codice_istat
    where rank=1
) zona_clim on ca_p.t_codice_pdr = zona_clim.t_codice_pdr
) result
where
--ZONA_CLIMATICA IS NOT NULL
--AND
CODICE_PDR = '${pdr}'
;