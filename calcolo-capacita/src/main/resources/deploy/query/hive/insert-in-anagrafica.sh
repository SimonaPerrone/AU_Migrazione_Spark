export CREATE_ANAGRAFICA=$(cat <<-EOF

CREATE TABLE IF NOT EXISTS ${hive.table.anagrafica.db}.${hive.table.anagrafica.name}(
n_id_pdr string,
data_calc string,
n_id_remi_anagrafica string,
t_codice_pdr string,
n_prelievo_annuo double,
t_z double,
t_pmax	double,
t_trattamento_settlement string)
STORED AS PARQUET;

insert overwrite table ${hive.table.anagrafica.db}.${hive.table.anagrafica.name}

select
 pdr.n_id_pdr,
 pdr.data_calc,
 pdr.n_id_remi_anagrafica,
 pdr.t_codice_pdr,
 pdr.n_prelievo_annuo,
 remi.t_z,
 remi.t_pmax,
 pdr.t_trattamento_settlement

from ${hive.table.clg_perimetro_pdr_gm_view.db}.${hive.table.clg_perimetro_pdr_gm_view.name} pdr
join ${hive.table.clg_perimetro_remi_gm_view.db}.${hive.table.clg_perimetro_remi_gm_view.name} remi
on REMI.n_id_remi_anagrafica = PDR.n_id_remi_anagrafica and PDR.t_cod_prof= remi.t_cod_profilo

union all

select
 pdr.n_id_pdr,
 pdr.data_calc,
 pdr.n_id_remi_anagrafica,
 pdr.t_codice_pdr,
 pdr.n_prelievo_annuo,
 remi.t_z,
 remi.t_pmax,
 pdr.t_trattamento_settlement

from ${hive.table.clg_perimetro_pdr_gm_view.db}.${hive.table.clg_perimetro_pdr_gm_view.name} pdr
join ${hive.table.clg_perimetro_remi_gm_view.db}.${hive.table.clg_perimetro_remi_gm_view.name} remi
on REMI.n_id_remi_anagrafica = PDR.n_id_remi_anagrafica and remi.t_cod_profilo ="NULL";
EOF
)

