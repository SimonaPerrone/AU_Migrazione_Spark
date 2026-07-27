export STORICI_FUNZIONALI_HIVE_PARAMETERS=$(cat <<-END
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
SET hive.exec.max.created.files=500000;
SET hive.merge.mapredfiles=true;
END
)
export STORICI_1_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with rs_timestamp as (
select
    nvl(d_data_apertura_m,'null')
    , nvl(t_cod_contr_disp,'null')
    , nvl(n_id_pratica,0.0)
    , nvl(pod14,'null')
    , nvl(d_data_decorrenza,'null')
    , nvl(n_id_utente_distr,'null')
    , nvl(t_protocollo,'null')
    , nvl(n_id_utente_udd,'null')
    , nvl(da_attivare_mese_succ,0.0)
    , nvl(attivati_nel_mese,0.0)
    , nvl(split_pod,0.0)
    , nvl(t_codice_pod,'null')
    , nvl(d_anno_mese,'null')
    , nvl(trattamento_online,'null')
    , nvl(tipo_misuratore_last,'null')
    , nvl(piva_distr,'null')
    , nvl(piva_udd,'null')
    , '${INGESTION_TIMESTAMP}' as d_creazione
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}_tb_tmp
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}
partition (annomese_sw)
select *
from rs_timestamp
END
)
export STORICI_2_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with rs_latest as (
    select *
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}
    where 1=1
    $(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
    $(! [[ -z "${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
), SW_CON_TRA_VIEW_RS_anno_mese as (
    select *
    from rs_latest
    full join (
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',0),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-1),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-2),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-3),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-4),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-5),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-6),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-7),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-8),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-9),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-10),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-11),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-12),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-13),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-14),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
    ) anno_mese_calcs on 1 = 1
), pod_misur_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_pod order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_MISURE_TABLE_NAME}
), pod_misur_latest as (
    select
        n_id_pod
        , d_anno_mese
        , t_trattamento
        , t_trattamento_succ
        , n_consumo_annuo
        , t_nota
        , d_aggiornamento
        , n_id_traccia
        , n_id_s_prec
    from pod_misur_ranking
    where d_aggiornamento_next is null
), pod_misur_storico_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_scheda order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_STORICO_MISURE_TABLE_NAME}
), pod_misur_storico_latest as (
    select
        n_id_scheda
        , n_id_pod
        , d_anno_mese
        , t_trattamento
        , t_trattamento_succ
        , n_consumo_annuo
        , t_nota
        , d_aggiornamento
        , d_archiviazione
        , n_id_traccia
        , n_id_s_prec
        , n_id_s_succ
        , b_valido
    from pod_misur_storico_ranking
    where d_aggiornamento_next is null
), sw_trat_online as (
    select a.*
        , case
              when a.anno_mese_calc >= onl.d_Anno_mese
              then onl.D_ANNO_MESE
              else null
          end as D_ANNO_MESE_onl
        , case
            when onl.d_Anno_mese = a.anno_mese_calc
            then onl.T_tRATTAMENTO
            when a.anno_mese_calc > onl.d_Anno_mese
            then onl.T_TRATTAMENTO_SUCC
            else null
        end as TRATTAMENTO_ONLINE_2
    from SW_CON_TRA_VIEW_RS_anno_mese a
    left join (
        select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
            , X.N_ID_POD
            , from_unixtime(unix_timestamp(D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS') as D_ANNO_MESE
            , T_TRATTAMENTO
            , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
            , X.N_CONSUMO_ANNUO
            , X.T_NOTA
            , X.D_AGGIORNAMENTO
            , X.N_ID_TRACCIA
            , X.N_ID_S_PREC
        from pod_misur_latest X
        inner join ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME} POD on (X.N_ID_POD = POD.N_ID_POD)
        ) onl on a.pod14 = onl.t_codice_pod
), data as (
    select D_DATA_APERTURA_M
        , T_COD_CONTR_DISP
        , N_ID_PRATICA
        , POD14
        , D_DATA_DECORRENZA
        , N_ID_UTENTE_DISTR
        , T_PROTOCOLLO
        , N_ID_UTENTE_UDD
        , DA_ATTIVARE_MESE_SUCC
        , ATTIVATI_NEL_MESE
        , SPLIT_POD
        , T_CODICE_POD
        , D_ANNO_MESE
        , PIVA_DISTR
        , PIVA_UDD
        , ANNO_MESE_CALC
        , NVL(TRATTAMENTO_ONLINE_2, TRATTAMENTO_STORICO_2) as TRATTAMENTO
        , ANNOMESE_SW
        , D_CREAZIONE
    from (
        select t_onl.*
            , case
                when t_onl.anno_mese_calc >= from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS')
                then x.D_aGGIORNAMENTO
                else '0'
            end as D_aGGIORNAMENTO_ofl
            , case
                when t_onl.anno_mese_calc >= from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS')
                then from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS')
                else null
            end as D_ANNO_MESE_ofl
            , case
                when from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS') = t_onl.anno_mese_calc
                    then x.T_tRATTAMENTO
                when t_onl.anno_mese_calc > from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS')
                then NVL(x.T_TRATTAMENTO_SUCC, x.T_tRATTAMENTO)
                else null
                end as TRATTAMENTO_STORICO_2
            , max(case
                when t_onl.anno_mese_calc >= from_unixtime(unix_timestamp(x.D_ANNO_MESE,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS')
                then x.D_aGGIORNAMENTO
                else '0'
            end) over (partition by X.N_ID_POD, t_onl.anno_mese_calc) as D_AGGIORNAMENTO_max
        from sw_trat_online t_onl
        left join ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME} POD on t_onl.pod14 = SUBSTR(pod.T_CODICE_POD, 1, 14)
        left join pod_misur_storico_latest X on (X.N_ID_POD = POD.N_ID_POD)
        ) data_before
    where D_aGGIORNAMENTO_ofl = D_AGGIORNAMENTO_max
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME}
partition (annomese_sw)
select
    nvl(d_data_apertura_m,'null')
    , nvl(t_cod_contr_disp,'null')
    , nvl(n_id_pratica,0.0)
    , nvl(pod14,'null')
    , nvl(d_data_decorrenza,'null')
    , nvl(n_id_utente_distr,'null')
    , nvl(t_protocollo,'null')
    , nvl(n_id_utente_udd,'null')
    , nvl(da_attivare_mese_succ,0.0)
    , nvl(attivati_nel_mese,0.0)
    , nvl(split_pod,0.0)
    , nvl(t_codice_pod,'null')
    , nvl(d_anno_mese,'null')
    , nvl(piva_distr,'null')
    , nvl(piva_udd,'null')
    , nvl(anno_mese_calc,'null')
    , nvl(trattamento,'null')
    , d_creazione
    , annomese_sw
from data
END
)
export STORICI_3_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with rs_latest as (
    select *
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}
    where 1=1
    $(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
    $(! [[ -z "${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
), SW_CON_TRA_VIEW_RS_anno_mese as (
    select *
    from rs_latest
    full join (
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',0),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-1),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-2),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-3),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-4),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-5),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-6),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-7),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-8),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-9),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-10),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-11),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-12),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-13),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
        union all
        select from_unixtime(unix_timestamp(add_months('${STORICI_SWITCHING_DELTA_FIRST_DAY}',-14),'yyyy-MM-dd'),'yyyy-MM-dd HH:mm:ss.SSS') as anno_mese_calc
    ) anno_mese_calcs on 1 = 1
), pod_tecn_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_pod order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TECN_TABLE_NAME}
), pod_tecn_latest as (
    select
        n_id_pod
        , n_potenza_disponibile
        , n_potenza_impegnata
        , n_tensione
        , t_tipo_misuratore
        , n_k_trasformazione
        , d_inst_misuratore
        , d_rimoz_misuratore
        , t_nota
        , d_aggiornamento
        , n_id_traccia
        , n_id_s_prec
        , n_num_cifre_ea
        , n_num_cifre_er
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
        , t_tipo_pod
        , d_fine_tipo_pod
        , d_oper_misurator_att
        , d_oper_misurator_rea
        , d_oper_misurator_pot
        , t_motivazione
    from pod_tecn_ranking
    where d_aggiornamento_next is null
), pod_tecn_storico_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_scheda order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_STORICO_TECN_TABLE_NAME}
), pod_tecn_storico_latest as (
    select
        n_id_scheda
        , n_id_pod
        , n_potenza_disponibile
        , n_potenza_impegnata
        , n_tensione
        , t_tipo_misuratore
        , n_k_trasformazione
        , d_inst_misuratore
        , d_rimoz_misuratore
        , t_nota
        , d_aggiornamento
        , d_archiviazione
        , n_id_traccia
        , n_id_s_prec
        , n_id_s_succ
        , b_valido
        , n_num_cifre_ea
        , n_num_cifre_er
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
        , t_tipo_pod
        , d_fine_tipo_pod
        , d_oper_misurator_att
        , d_oper_misurator_rea
        , d_oper_misurator_pot
        , t_motivazione
    from pod_tecn_storico_ranking
    where d_aggiornamento_next is null
), sw_tipo_mis_online as (
    select sw.*
        , case
            when nvl(from_unixtime(unix_timestamp(t_mis.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw.anno_mese_calc
            then t_mis.T_TIPO_MISURATORE
            else null
        end as tipomis_o
        , case
            when nvl(from_unixtime(unix_timestamp(t_mis.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw.anno_mese_calc
            then t_mis.D_INST_MISURATOR_ATT
            else null
        end as data_att_o
    from SW_CON_TRA_VIEW_RS_anno_mese sw
    inner join (
        select n_id_pod
            , substr(T_CODICE_POD, 1, 14) as pod14
        from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME}
        ) pod on sw.pod14 = pod.pod14
    left join pod_tecn_latest t_mis on pod.n_id_pod = t_mis.n_id_pod
), data as (
    select
        D_DATA_APERTURA_M
        , T_COD_CONTR_DISP
        , N_ID_PRATICA
        , POD14
        , D_DATA_DECORRENZA
        , N_ID_UTENTE_DISTR
        , T_PROTOCOLLO
        , N_ID_UTENTE_UDD
        , DA_ATTIVARE_MESE_SUCC
        , ATTIVATI_NEL_MESE
        , SPLIT_POD
        , T_CODICE_POD
        , D_ANNO_MESE
        , TRATTAMENTO_ONLINE
        , PIVA_DISTR
        , PIVA_UDD
        , ANNO_MESE_CALC
        , NVL(TIPOMIS_O, TIPOMIS_S) as TIPO_MISURATORE
        , DATA_INST as DATA_INST_MIS
        , ANNOMESE_SW
        , D_CREAZIONE
    from (
        select a.*
            , DENSE_RANK() over (
                partition by a.pod14
                , a.anno_mese_calc order by a.DATA_inst_order desc
                    , a.d_aggiornamento desc
                    , a.n_id_traccia desc
                ) as ultimo_tipo_mis
        from (
            select sw_o.*
                , case
                    when nvl(from_unixtime(unix_timestamp(t_mis_s.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw_o.anno_mese_calc
                    then t_mis_s.T_TIPO_MISURATORE
                    else null
                end as tipomis_s
                , case
                    when nvl(from_unixtime(unix_timestamp(t_mis_s.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw_o.anno_mese_calc
                    then nvl(sw_o.data_att_o, t_mis_s.D_INST_MISURATOR_ATT)
                    else sw_o.data_att_o
                end as DATA_inst
                , case
                    when nvl(from_unixtime(unix_timestamp(t_mis_s.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw_o.anno_mese_calc
                    then nvl(nvl(sw_o.data_att_o, t_mis_s.D_INST_MISURATOR_ATT),'1200-01-01 00:00:00.0')
                    else nvl(sw_o.data_att_o,'1200-01-01 00:00:00.0')
                end as DATA_inst_order
                , case
                    when nvl(from_unixtime(unix_timestamp(t_mis_s.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw_o.anno_mese_calc
                    then t_mis_s.d_aggiornamento
                    else null
                end as d_aggiornamento
                , case
                    when nvl(from_unixtime(unix_timestamp(t_mis_s.D_INST_MISURATOR_ATT,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss.SSS'), '1900-01-01 00:00:00.000') <= sw_o.anno_mese_calc
                    then n_id_traccia
                    else null
                end as n_id_traccia
            from sw_tipo_mis_online sw_o
            inner join (
                select n_id_pod
                    , substr(T_CODICE_POD, 1, 14) as pod14
                from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME}
                ) pod on sw_o.pod14 = pod.pod14
            left join pod_tecn_storico_latest t_mis_s on pod.n_id_pod = t_mis_s.n_id_pod
            ) a
        ) b
    where ultimo_tipo_mis = 1
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_MISUR_TABLE_NAME}
partition (annomese_sw)
select
    nvl(d_data_apertura_m,'null')
    , nvl(t_cod_contr_disp,'null')
    , nvl(n_id_pratica,0.0)
    , nvl(pod14,'null')
    , nvl(d_data_decorrenza,'null')
    , nvl(n_id_utente_distr,'null')
    , nvl(t_protocollo,'null')
    , nvl(n_id_utente_udd,'null')
    , nvl(da_attivare_mese_succ,0.0)
    , nvl(attivati_nel_mese,0.0)
    , nvl(split_pod,0.0)
    , nvl(t_codice_pod,'null')
    , nvl(d_anno_mese,'null')
    , nvl(trattamento_online,'null')
    , nvl(piva_distr,'null')
    , nvl(piva_udd,'null')
    , nvl(anno_mese_calc,'null')
    , nvl(tipo_misuratore,'null')
    , nvl(data_inst_mis,'null')
    , d_creazione
    , annomese_sw
from data
END
)
export STORICI_4_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with SW_CON_TRA_VIEW_RS_anno_mese
as (
    select *
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}
    where 1=1
    $(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
    $(! [[ -z "${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
), pod_stato_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_pod order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_STATO_TABLE_NAME}
), pod_stato_latest as (
    select
        n_id_pod
        , t_stato_attivazione
        , d_attivazione
        , d_disattivazione
        , t_causale_no_riattiv
        , t_causale_no_disattiv
        , t_stato_sosp
        , d_sospensione
        , d_revoca_sosp
        , t_causale_no_sosp
        , t_switching
        , t_nota
        , d_aggiornamento
        , n_id_traccia
        , n_id_s_prec
        , t_cod_disattivazione
    from pod_stato_ranking
    where d_aggiornamento_next is null
), pod_stato_storico_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_scheda order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_STORICO_STATO_TABLE_NAME}
), pod_stato_storico_latest as (
    select
        n_id_scheda
        , n_id_pod
        , t_stato_attivazione
        , d_attivazione
        , d_disattivazione
        , t_causale_no_riattiv
        , t_causale_no_disattiv
        , t_stato_sosp
        , d_sospensione
        , d_revoca_sosp
        , t_causale_no_sosp
        , t_switching
        , t_nota
        , d_aggiornamento
        , d_archiviazione
        , n_id_traccia
        , n_id_s_prec
        , n_id_s_succ
        , b_valido
        , t_cod_disattivazione
    from pod_stato_storico_ranking
    where d_aggiornamento_next is null
), data as (
    select MM.*
        , NVL(d_attivazione, '1900-01-01 00:00:00') as d_attivazione
        , NVL(d_disattivazione, '3099-01-01 00:00:00') as d_disattivazione
    from SW_CON_TRA_VIEW_RS_anno_mese MM
        inner join ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME} POD on (MM.POD14 = SUBSTR(POD.T_CODICE_POD, 1, 14))
        inner join pod_stato_latest S on (S.N_ID_POD = POD.N_ID_POD)

    union all

    select MM.*
        , NVL(d_attivazione, '1900-01-01 00:00:00') d_attivazione
        , NVL(d_disattivazione, '3099-01-01 00:00:00') d_disattivazione
    from SW_CON_TRA_VIEW_RS_anno_mese MM
        inner join ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME} POD on (MM.POD14 = SUBSTR(POD.T_CODICE_POD, 1, 14))
        inner join pod_stato_storico_latest S on (S.N_ID_POD = POD.N_ID_POD)
    where t_stato_attivazione = 'N'
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_STATO_TABLE_NAME}
partition (annomese_sw)
select
    nvl(d_data_apertura_m,'null')
    , nvl(t_cod_contr_disp,'null')
    , nvl(n_id_pratica,0.0)
    , nvl(pod14,'null')
    , nvl(d_data_decorrenza,'null')
    , nvl(n_id_utente_distr,'null')
    , nvl(t_protocollo,'null')
    , nvl(n_id_utente_udd,'null')
    , nvl(da_attivare_mese_succ,0.0)
    , nvl(attivati_nel_mese,0.0)
    , nvl(split_pod,0.0)
    , nvl(t_codice_pod,'null')
    , nvl(d_anno_mese,'null')
    , nvl(trattamento_online,'null')
    , nvl(tipo_misuratore_last,'null')
    , nvl(piva_distr,'null')
    , nvl(piva_udd,'null')
    , nvl(d_attivazione,'null')
    , nvl(d_disattivazione,'null')
    , d_creazione
    , annomese_sw
from data
END
)
export STORICI_5_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with SW_storici_funzionali
as (
    select *
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RS_TABLE_NAME}
    where annomese_sw >= '202101'
    $(! [[ -z "${INGESTION_TIMESTAMP}" ]] && echo -n "and d_creazione = '${INGESTION_TIMESTAMP}'")
    $(! [[ -z "${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]}" ]] && echo -n 'and annomese_sw in ('\' && echo -n $(echo ${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
), misur_2g_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_misuratore_2g order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_MISURATORE_2G_TABLE_NAME}
), misur_2g_latest as (
    select
        n_id_misuratore_2g
        , n_id_pod
        , b_vis_fasce
        , b_vis_venditore
        , b_vis_telefonov
        , b_vis_datainicontr
        , b_vis_datainiziofreezing
        , b_vis_messaggicliente
        , b_vis_codcli
        , t_codcli
        , t_venditore
        , t_telefonov
        , d_data_inicontr
        , d_data_iniziofreezing
        , t_messaggio_cliente_1
        , t_messaggio_cliente_2
        , t_messaggio_cliente_3
        , t_messaggio_cliente_4
        , t_messaggio_cliente_5
        , n_num_fasce
        , d_inizio_validita
        , d_fine_validita
        , t_nota
        , d_aggiornamento
        , n_id_traccia
        , n_id_s_prec
        , d_data_rif
        , t_tipo_configurazione
    from misur_2g_ranking
    where d_aggiornamento_next is null
), fasce_misur_2g_ranking as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_fasce_misuratore_2g order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FASCE_MISURATORE_2G_TABLE_NAME}
), fasce_misur_2g_latest as (
    select
        n_id_fasce_misuratore_2g
        , n_id_misuratore
        , n_cod_giorno_2g
        , d_data_giorno
        , n_fascia_1
        , n_fine_fascia_1
        , n_fascia_2
        , n_fine_fascia_2
        , n_fascia_3
        , n_fine_fascia_3
        , n_fascia_4
        , n_fine_fascia_4
        , n_fascia_5
        , n_fine_fascia_5
        , n_fascia_6
        , n_fine_fascia_6
        , n_fascia_7
        , n_fine_fascia_7
        , n_fascia_8
        , n_fine_fascia_8
        , n_fascia_9
        , n_fine_fascia_9
        , n_fascia_10
        , n_fine_fascia_10
        , t_nota
        , d_aggiornamento
        , n_id_traccia
        , n_id_s_prec
        , d_data_rif
    from fasce_misur_2g_ranking
    where d_aggiornamento_next is null
), data as (
    select distinct zz.pod14 as pod_config
        , zz.annomese_sw
        , zz.d_data_decorrenza
        , case
            when T_TIPO_CONFIGURAZIONE = 'C'
                then 'SI'
            else 'NO'
            end T_TIPO_CONFIGURAZIONE
        , zz.d_creazione as d_creazione
    from SW_storici_funzionali ZZ
        inner join ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME} pod on (substr(POD.T_CODICE_POD, 1, 14) = ZZ.POD14)
        inner join misur_2g_latest xx on (xx.n_id_pod = pod.n_id_pod)
        inner join fasce_misur_2g_latest x on (xx.n_id_MISURATORE_2G = x.N_ID_MISURATORE)
    where xx.d_fine_validita is null
        and T_TIPO_CONFIGURAZIONE = 'C'
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_STORICI_RICONF_TABLE_NAME}
partition (annomese_sw)
select
    nvl(pod_config,'null')
    , nvl(d_data_decorrenza,'null')
    , nvl(t_tipo_configurazione,'null')
    , d_creazione
    , annomese_sw
from data
END
)
export FUNZIONALI_1_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with fu_timestamp as (
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
    , t_diritto_tutela
    , b_disalimentabilita
    , piva_distr
    , piva_udd
    , t_residente
    , t_tariffa_distr
    , t_tipo_configurazione
    , '${INGESTION_TIMESTAMP}' as d_creazione
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME}_tb_tmp
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_TABLE_NAME}
partition (annomese_sw)
select *
from fu_timestamp
END
)
export FUNZIONALI_2_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
with fu_na_timestamp as (
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
    , t_diritto_tutela
    , b_disalimentabilita
    , piva_distr
    , piva_udd
    , t_residente
    , t_tariffa_distr
    , t_tipo_configurazione
    , '${INGESTION_TIMESTAMP}' as d_creazione
    , annomese_sw
from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_NA_TABLE_NAME}_tb_tmp
)
insert into ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_FUNZIONALI_NA_TABLE_NAME}
partition (annomese_sw)
select *
from fu_na_timestamp
END
)
export RCU_POD_TMP_QUERY=$(cat <<-END
${STORICI_FUNZIONALI_HIVE_PARAMETERS}
drop table if exists ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME};
create table if not exists ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TMP_TABLE_NAME}
stored as parquet
as
with rcu_pod_latest as (
    select
        *
        , lead(d_aggiornamento) over (partition by n_id_pod order by d_aggiornamento asc, d_creazione asc) as d_aggiornamento_next
    from ${SWITCHING_EE_HIVE_DB_NAME}.${SWITCHING_EE_HIVE_POD_TABLE_NAME}
)
select
    n_id_pod
    , t_codice_pod
    , t_area_rif
    , b_rich_indennizzo
    , b_rich_prest_distr
    , n_id_indirizzo
    , t_nota
    , d_aggiornamento
    , n_id_traccia
    , n_id_s_prec
    , n_id_ind_forn
from rcu_pod_latest
where d_aggiornamento_next is null
END
)
