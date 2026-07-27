export FUNZIONALI_FU_QUERY=$(cat <<-END
select
    from_unixtime(unix_timestamp(nvl(d_data_apertura_m,'1492-12-31 00:00:00.0'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_data_apertura_m
    , t_cod_contr_disp
    , n_id_pratica
    , pod14
    , d_data_decorrenza
    , n_id_utente_distr
    , t_protocollo
    , n_id_utente_udd
    , da_attivare_mese_succ
    , attivati_nel_mese
    , split_pod
    , t_codice_pod
    , from_unixtime(unix_timestamp(nvl(d_anno_mese,'1492-12-31 00:00:00.0'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_anno_mese
    , trattamento_online
    , pod
    , case
          when t_tipo_misuratore = 'O'
          then 'E'
          else t_tipo_misuratore
      end as t_tipo_misuratore
    , t_area_rif
    , n_tensione
    , n_potenza_disponibile
    , n_potenza_impegnata
    , n_k_trasfor_att
    , n_k_trasfor_rea
    , n_k_trasfor_pot
    , t_mat_misuratore_att
    , t_mat_misuratore_rea
    , t_mat_misuratore_pot
    , from_unixtime(unix_timestamp(d_inst_misurator_att, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_att
    , from_unixtime(unix_timestamp(d_inst_misurator_rea, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_rea
    , from_unixtime(unix_timestamp(d_inst_misurator_pot, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_pot
    , n_num_cifre_att
    , n_num_cifre_rea
    , n_num_cifre_pot
    , b_presenza_mis
    , b_gest_forfait
    , from_unixtime(unix_timestamp(d_regime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_regime
    , pod_fornitura
    , regexp_replace(
        regexp_replace(case
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%TD%'
                    then 'MT'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%AT%'
                    then 'S'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%MT%'
                    then 'S'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%BT%'
                    then 'MT'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and (
                        t_tariffa_distr = 'null'
                        or t_tariffa_distr is null
                        )
                    then 'MT'
                else t_diritto_tutela
                end, '01', 'MT')
            , '02', 'S') as servizio_tutela
    , case
          when b_disalimentabilita = 'Y'
          then 'SI'
          when b_disalimentabilita = 'N'
          then 'NO'
          else b_disalimentabilita
      end as b_disalimentabilita
    , piva_distr
    , piva_udd
    , case
          when t_residente = 'Y'
          then 'SI'
          when t_residente = 'N'
          then 'NO'
          else t_residente
      end as t_residente
    , t_tariffa_distr
    , t_tipo_configurazione
    , case
          when t_tipo_misuratore = 'G'
          then 'F2G'
          when t_tipo_misuratore != 'G'
              AND trattamento_online = 'O'
          then 'SOF'
          when t_tipo_misuratore != 'G'
              AND trattamento_online != 'O'
          then 'SNF'
          else 'XXX'
      end as nome_flusso
    , false as is_nuova_attivazione
    , d_creazione
    , '${LOADING_TIMESTAMP}' as d_caricamento
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME}
where 1=1
$(! [[ -z "${NO_SWITCHING_DATES}" ]] && echo -n "and 1=0")
$(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
$(! [[ -z "${SWITCHING_DATE_ARRAY[@]}" ]] && echo -n 'and from_unixtime(unix_timestamp(nvl(d_data_decorrenza,'\''1492-12-31'\''), '\''yyyy-MM-dd'\''), '\''yyyyMMdd'\'') in ('\' && echo -n $(echo ${SWITCHING_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_DISTR_ARRAY[@]}" ]] && echo -n 'and piva_distr in ('\' && echo -n $(echo ${PIVA_DISTR_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_UDD_ARRAY[@]}" ]] && echo -n 'and piva_udd in ('\' && echo -n $(echo ${PIVA_UDD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_COUPLES_ARRAY[@]}" ]] && echo -n 'and (' && echo -n "piva_distr = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 1)' and piva_udd = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 2)'" && for couple in "${PIVA_COUPLES_ARRAY[@]:1}"; do echo -n " or piva_distr = '$(echo $couple | cut -d ',' -f 1)' and piva_udd = '$(echo $couple | cut -d ',' -f 2)'"; done && echo -n ')')
$(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and pod in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${SWITCHING_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
END
)
export FUNZIONALI_NA_QUERY=$(cat <<-END
select
    from_unixtime(unix_timestamp(nvl(d_data_apertura_m,'1492-12-31 00:00:00.0'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_data_apertura_m
    , t_cod_contr_disp
    , n_id_pratica
    , pod14
    , d_data_decorrenza
    , n_id_utente_distr
    , t_protocollo
    , n_id_utente_udd
    , da_attivare_mese_succ
    , attivati_nel_mese
    , split_pod
    , t_codice_pod
    , from_unixtime(unix_timestamp(nvl(d_anno_mese,'1492-12-31 00:00:00.0'), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_anno_mese
    , trattamento_online
    , pod
    , case
          when t_tipo_misuratore = 'O'
          then 'E'
          else t_tipo_misuratore
      end as t_tipo_misuratore
    , t_area_rif
    , n_tensione
    , n_potenza_disponibile
    , n_potenza_impegnata
    , n_k_trasfor_att
    , n_k_trasfor_rea
    , n_k_trasfor_pot
    , t_mat_misuratore_att
    , t_mat_misuratore_rea
    , t_mat_misuratore_pot
    , from_unixtime(unix_timestamp(d_inst_misurator_att, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_att
    , from_unixtime(unix_timestamp(d_inst_misurator_rea, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_rea
    , from_unixtime(unix_timestamp(d_inst_misurator_pot, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_inst_misurator_pot
    , n_num_cifre_att
    , n_num_cifre_rea
    , n_num_cifre_pot
    , b_presenza_mis
    , b_gest_forfait
    , from_unixtime(unix_timestamp(d_regime, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as d_regime
    , pod_fornitura
    , regexp_replace(
        regexp_replace(case
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%TD%'
                    then 'MT'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%AT%'
                    then 'S'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%MT%'
                    then 'S'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and t_tariffa_distr like '%BT%'
                    then 'MT'
                when nvl(t_diritto_tutela,'') not in ('01', '02', 'MT', 'S', 'TG','TM')
                    and (
                        t_tariffa_distr = 'null'
                        or t_tariffa_distr is null
                        )
                    then 'MT'
                else t_diritto_tutela
                end, '01', 'MT')
            , '02', 'S') as servizio_tutela
    , case
          when b_disalimentabilita = 'Y'
          then 'SI'
          when b_disalimentabilita = 'N'
          then 'NO'
          else b_disalimentabilita
      end as b_disalimentabilita
    , piva_distr
    , piva_udd
    , case
          when t_residente = 'Y'
          then 'SI'
          when t_residente = 'N'
          then 'NO'
          else t_residente
      end as t_residente
    , t_tariffa_distr
    , t_tipo_configurazione
    , case
          when t_tipo_misuratore = 'G'
          then 'F2G'
          when t_tipo_misuratore != 'G'
              AND trattamento_online = 'O'
          then 'SOF'
          when t_tipo_misuratore != 'G'
              AND trattamento_online != 'O'
          then 'SNF'
          else 'XXX'
      end as nome_flusso
    , true as is_nuova_attivazione
    , d_creazione
    , '${LOADING_TIMESTAMP}' as d_caricamento
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_NA_TABLE_NAME}
where 1=1
$(! [[ -z "${NO_NEW_ACTIVATION_DATES}" ]] && echo -n "and 1=0")
$(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
$(! [[ -z "${NEW_ACTIVATION_DATE_ARRAY[@]}" ]] && echo -n 'and from_unixtime(unix_timestamp(nvl(d_data_decorrenza,'\''1492-12-31'\''), '\''yyyy-MM-dd'\''), '\''yyyyMMdd'\'') in ('\' && echo -n $(echo ${NEW_ACTIVATION_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_DISTR_ARRAY[@]}" ]] && echo -n 'and piva_distr in ('\' && echo -n $(echo ${PIVA_DISTR_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_UDD_ARRAY[@]}" ]] && echo -n 'and piva_udd in ('\' && echo -n $(echo ${PIVA_UDD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_COUPLES_ARRAY[@]}" ]] && echo -n 'and (' && echo -n "(piva_distr = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 1)' and piva_udd = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 2)')" && for couple in "${PIVA_COUPLES_ARRAY[@]:1}"; do echo -n " or (piva_distr = '$(echo $couple | cut -d ',' -f 1)' and piva_udd = '$(echo $couple | cut -d ',' -f 2)')"; done && echo -n ')')
$(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and pod in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
END
)
export FUNZIONALI_START_SNIPPET=$(cat <<-END
SET mapreduce.map.memory.mb=2000;
SET mapreduce.map.java.opts.max.heap=1700;
SET mapreduce.map.java.opts=-Xmx1700m;
SET mapreduce.reduce.memory.mb=2000;
SET mapreduce.reduce.java.opts=-Xmx1700m;
SET mapreduce.reduce.java.opts.max.heap=1700;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.exec.max.created.files = 500000;
SET hive.merge.mapredfiles=true;

with funzionali_fu as (
${FUNZIONALI_FU_QUERY}
), funzionali_na as (
${FUNZIONALI_NA_QUERY}
), funzionali_all as (
    select *
    from funzionali_fu
    union all
    select *
    from funzionali_na
), funzionali_all_lead as (
    select
        *
        , lead(d_creazione) over (
            partition by pod14, d_data_decorrenza, is_nuova_attivazione
            order by d_creazione asc
        ) as d_creazione_next
    from funzionali_all
), funzionali_all_latest as (
    select
        d_data_apertura_m
        , t_cod_contr_disp
        , n_id_pratica
        , pod14
        , d_data_decorrenza
        , n_id_utente_distr
        , t_protocollo
        , n_id_utente_udd
        , da_attivare_mese_succ
        , attivati_nel_mese
        , split_pod
        , t_codice_pod
        , d_anno_mese
        , trattamento_online
        , pod
        , t_tipo_misuratore
        , t_area_rif
        , n_tensione
        , n_potenza_disponibile
        , n_potenza_impegnata
        , n_k_trasfor_att
        , n_k_trasfor_rea
        , n_k_trasfor_pot
        , t_mat_misuratore_att
        , t_mat_misuratore_rea
        , t_mat_misuratore_pot
        , d_inst_misurator_att
        , d_inst_misurator_rea
        , d_inst_misurator_pot
        , n_num_cifre_att
        , n_num_cifre_rea
        , n_num_cifre_pot
        , b_presenza_mis
        , b_gest_forfait
        , d_regime
        , pod_fornitura
        , servizio_tutela
        , b_disalimentabilita
        , piva_distr
        , piva_udd
        , t_residente
        , t_tariffa_distr
        , t_tipo_configurazione
        , nome_flusso
        , is_nuova_attivazione
        , d_creazione
        , d_caricamento
        , annomese_sw
    from funzionali_all_lead
    where d_creazione_next is null
)
END
)
export FUNZIONALI_ALL_CHECK_QUERY=$(cat <<-END
${FUNZIONALI_START_SNIPPET}
select *
from funzionali_all_latest
limit 1;
END
)
export FUNZIONALI_ALL_INSERT_QUERY=$(cat <<-END
${FUNZIONALI_START_SNIPPET}
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_OUTPUT_TABLE_NAME}
partition (annomese_sw)
select *
from funzionali_all_latest;
END
)
export FUNZIONALI_ALL_SCARTI_QUERY=$(cat <<-END
SET mapreduce.map.memory.mb=2000;
SET mapreduce.map.java.opts.max.heap=1700;
SET mapreduce.map.java.opts=-Xmx1700m;
SET mapreduce.reduce.memory.mb=2000;
SET mapreduce.reduce.java.opts=-Xmx1700m;
SET mapreduce.reduce.java.opts.max.heap=1700;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=20000;
SET hive.exec.max.dynamic.partitions.pernode=20000;
SET hive.exec.max.created.files = 500000;
SET hive.merge.mapredfiles=true;
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_SCARTI_TABLE_NAME}
partition (annomese_sw)
select
    pod14
    , d_data_decorrenza
    , d_caricamento
    , nome_flusso
    , t_cod_contr_disp
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_OUTPUT_TABLE_NAME}
where (
    nome_flusso = 'XXX'
    or t_cod_contr_disp is null
    )
$(! [[ -z "${LOADING_TIMESTAMP}" ]] && echo -n "and d_caricamento = '${LOADING_TIMESTAMP}'")
$(! [[ -z "${ALL_DATES_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${ALL_DATES_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
END
)