export FUNZIONALI_1_ORACLE_TABLE_NAME='SW_CON_TRA_VIEW_FU_TB_TMP'
export FUNZIONALI_1_DELETE_ORACLE_QUERY="drop table ${FUNZIONALI_1_ORACLE_TABLE_NAME}"
export FUNZIONALI_1_ORACLE_QUERY=$(cat <<-END
create table ${FUNZIONALI_1_ORACLE_TABLE_NAME} COMPRESS FOR OLTP as
    with SW_storici_funzionali as (
            select /*+ PARALLEL (10) */ gg.*
                , to_number(REGEXP_REPLACE(pod14, '[^0-9]+', '')) as split_pod
            from (
                select to_char(trunc(t00.D_DATA_DECORRENZA), 'yyyymm') ANNOMESE_SW
                    , TRUNC(T01.D_DATA_aPERTURA, 'MONTH') D_DATA_aPERTURA_MM
                    , t00.T_COD_CONTR_DISP
                    , t00.n_id_pratica
                    , substr(t00.t_codice_pod, 1, 14) as pod14
                    , TRUNC(t00.d_data_decorrenza, 'MONTH') D_DATA_aPERTURA_M
                    , to_char(t00.n_id_distr) as n_id_utente_distr
                    , t01.t_protocollo
                    , to_char(t01.n_id_utente) as n_id_utente_udd
                    , 1 as DA_ATTIVARE_MESE_SUCC
                    , 0 as ATTIVATI_NEL_MESE
                    , to_char(d_data_decorrenza,'yyyy-mm-dd') as d_data_decorrenza
                from swtch.prt_se T00
                join USERAPPL.T001_APP_PRT_PRATICHE T01 on T00.N_ID_PRATICA = T01.N_ID_PRATICA
                where 1=1
                    $(! [[ -z "${FUNZIONALI_SWITCHING_ALL_MONTH}" ]] && echo -n 'and to_char(t00.D_DATA_DECORRENZA,'\''yyyymm'\'') in ('\' && echo -n $(echo ${FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${FUNZIONALI_SWITCHING_DATE_ARRAY[@]}" ]] && echo -n 'and to_char(t00.D_DATA_DECORRENZA,'\''yyyymmdd'\'') in ('\' && echo -n $(echo ${FUNZIONALI_SWITCHING_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and substr(t00.t_codice_pod, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    and b_ammissibile = 'Y'
                    and T01.T_STATO in ('IN CORSO','CHIUSA')
                    and T00.T_STATO not like '%E%'

                union all

                select to_char(trunc(t00.d_data_dec), 'yyyymm') ANNOMESE_SW
                    , TRUNC(T01.D_DATA_aPERTURA, 'MONTH') D_DATA_aPERTURA_MM
                    , t00.T_COD_CONTR_DISP
                    , t00.n_id_pratica
                    , substr(t00.t_codice_pod, 1, 14) as pod14
                    , TRUNC(t00.d_data_dec, 'MONTH') D_DATA_aPERTURA_M
                    , to_char(t00.n_id_distr) as n_id_utente_distr
                    , t01.t_protocollo
                    , to_char(t00.n_id_esercente) as n_id_utente_udd
                    , 1 as DA_ATTIVARE_MESE_SUCC
                    , 0 as ATTIVATI_NEL_MESE
                    , to_char(d_data_dec,'yyyy-mm-dd') as d_data_decorrenza
                from swtch.prt_ui T00
                join USERAPPL.T001_APP_PRT_PRATICHE T01 on T00.N_ID_PRATICA = T01.N_ID_PRATICA
                where 1=1
                    $(! [[ -z "${FUNZIONALI_SWITCHING_ALL_MONTH}" ]] && echo -n 'and to_char(t00.d_data_dec,'\''yyyymm'\'') in ('\' && echo -n $(echo ${FUNZIONALI_SWITCHING_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${FUNZIONALI_SWITCHING_DATE_ARRAY[@]}" ]] && echo -n 'and to_char(t00.d_data_dec,'\''yyyymmdd'\'') in ('\' && echo -n $(echo ${FUNZIONALI_SWITCHING_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and substr(t00.t_codice_pod, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    and T01.T_STATO in ('IN CORSO','CHIUSA')
                    and T00.T_STATO not like '%E%'
                ) GG
            )
        , SW_storici_funzionali_T as (
            select /*+ PARALLEL (15) */ T_CODICE_POD
                , D_ANNO_MESE
                , TRATTAMENTO_ONLINE
            from (
                select X.*
                    , RANK() over (
                        partition by SUBSTR(T_CODICE_POD, 1, 14) order by COL asc
                        ) ranking
                from (
                    select 'O' COL
                        , X.*
                        , case
                            when d_Anno_mese = TRUNC(sysdate, 'MONTH')
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_ONLINE
                    from (
                        select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                            , X.N_ID_POD
                            , D_ANNO_MESE
                            , T_TRATTAMENTO
                            , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                            , X.N_CONSUMO_ANNUO
                            , X.T_NOTA
                            , X.D_AGGIORNAMENTO
                            , X.N_ID_TRACCIA
                            , X.N_ID_S_PREC
                        from rcu.rcu_pod_misure X
                            , RCU.RCU_POD POD
                            , SW_storici_funzionali ZZ
                        where SUBSTR(T_CODICE_POD, 1, 14) = ZZ.POD14
                            and d_Anno_mese <= TRUNC(sysdate, 'MONTH')
                            and X.N_ID_POD = POD.N_ID_POD
                        ) x
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14

                    union all

                    select 'S' COL
                        , T_CODICE_POD
                        , "N_ID_POD"
                        , MAX_D_ANNO_MESE
                        , "T_TRATTAMENTO"
                        , NVL("T_TRATTAMENTO_SUCC", T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                        , "N_CONSUMO_ANNUO"
                        , "T_NOTA"
                        , "MAX_D_AGGIORNAMENTO"
                        , "N_ID_TRACCIA"
                        , N_ID_S_PREC
                        , case
                            when d_Anno_mese = TRUNC(sysdate, 'MONTH')
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_STORICO
                    from (
                        select *
                        from (
                            select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                                , X.N_ID_POD
                                , D_ANNO_MESE
                                , T_TRATTAMENTO
                                , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                                , N_CONSUMO_ANNUO
                                , X.T_NOTA
                                , X.D_AGGIORNAMENTO
                                , X.N_ID_TRACCIA
                                , X.N_ID_S_PREC
                                , max(X.D_aGGIORNAMENTO) over (
                                    partition by X.N_ID_POD
                                    , D_ANNO_MESE
                                    ) MAX_D_AGGIORNAMENTO
                                , max(D_ANNO_MESE) over (partition by X.N_ID_POD) mAX_D_ANNO_MESE
                            from rcuS.rcuS_podmisure X
                                , RCU.RCU_POD POD
                                , SW_storici_funzionali ZZ
                            where d_Anno_mese <= TRUNC(sysdate, 'MONTH')
                                and X.N_ID_POD = POD.N_ID_POD
                                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                            )
                        where D_aGGIORNAMENTO = MAX_D_AGGIORNAMENTO
                            and D_ANNO_MESE = mAX_D_ANNO_MESE
                        ) X
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14
                    ) X
                )
            where RANKING = 1
            )
        , pod_tecn as (
            select zz.pod14 as pod
                , t_tipo_misuratore
                , pod.t_Area_Rif
                , n_Tensione
                , n_potenza_disponibile
                , n_potenza_impegnata
                , n_k_trasfor_Att
                , n_k_trasfor_rea
                , n_k_trasfor_pot
                , t_mat_misuratore_Att
                , t_mat_misuratore_rea
                , t_mat_misuratore_pot
                , d_inst_misurator_Att
                , d_inst_misurator_rea
                , d_inst_misurator_pot
                , n_num_cifre_att
                , n_num_cifre_rea
                , n_num_cifre_pot
                , B_PRESENZA_MIS
                , B_GEST_FORFAIT
                , d_oper_misurator_att as d_regime
            from rcu.rcu_pod_tecn x
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
            )
        , fornitura as (
            select zz.pod14 as pod_fornitura
                , nvl(x.t_servizio_tutela_sii,x.t_diritto_tutela) as t_diritto_tutela
                , x.b_disalimentabilita
            from rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
            )
        , tariffa as (
            select zz.pod14 as pod_tariffa
                , xx.t_tariffa_distr
            from rcu.rcu_TARIFFA xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.b_ultima = 'Y'
                and Xx.B_VALIDO = 'Y'
                and xx.n_id_fornitura = x.n_id_fornitura
            )
        , residenza as (
            select zz.pod14 as pod_residenza
                , xx.t_RESIDENTE
            from rcu.rcu_residenza xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.b_ultima = 'Y'
                and Xx.B_VALIDO = 'Y'
                and xx.n_id_fornitura = x.n_id_fornitura
            )
        , configurazione as (
            select distinct zz.pod14 as pod_config
                , case
                    when T_TIPO_CONFIGURAZIONE = 'C'
                        then 'SI'
                    else 'NO'
                    end T_TIPO_CONFIGURAZIONE
            from RCU.RCU_MISURATORE_2G xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , RCU.RCU_FASCE_MISURATORE_2G x
            where xx.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.d_fine_validita is null
                and xx.n_id_MISURATORE_2G = x.N_ID_MISURATORE
            )

select ANNOMESE_SW
    , D_DATA_APERTURA_M
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
    , POD
    , T_TIPO_MISURATORE
    , T_AREA_RIF
    , N_TENSIONE
    , N_POTENZA_DISPONIBILE
    , N_POTENZA_IMPEGNATA
    , N_K_TRASFOR_ATT
    , N_K_TRASFOR_REA
    , N_K_TRASFOR_POT
    , T_MAT_MISURATORE_ATT
    , T_MAT_MISURATORE_REA
    , T_MAT_MISURATORE_POT
    , D_INST_MISURATOR_ATT
    , D_INST_MISURATOR_REA
    , D_INST_MISURATOR_POT
    , N_NUM_CIFRE_ATT
    , N_NUM_CIFRE_REA
    , N_NUM_CIFRE_POT
    , B_PRESENZA_MIS
    , B_GEST_FORFAIT
    , D_REGIME
    , POD_FORNITURA
    , T_DIRITTO_TUTELA
    , B_DISALIMENTABILITA
    , PIVA_DISTR
    , PIVA_UDD
    , T_RESIDENTE
    , T_TARIFFA_DISTR
    , T_TIPO_CONFIGURAZIONE
from (
    select jj.*
        , kkkk.t_RESIDENTE
        , kk.t_tariffa_distr
        , NVL(T_TIPO_CONFIGURAZIONE, 'NO') as T_TIPO_CONFIGURAZIONE
        , mmmm.*
    from (
        select X.*
            , K.*
            , KK.*
            , DISTR.T_PIVA as PIVA_DISTR
            , UDD.T_PIVA as PIVA_UDD
        from SW_storici_funzionali X
            , pod_tecn k
            , fornitura KK
            , RCU.RCU_AZIENDA DISTR
            , RCU.RCU_AZIENDA UDD
        where k.pod = x.pod14
            and KK.POD_fornitura = x.pod14
            and KK.POD_fornitura = x.pod14
            and n_id_utente_distr = DISTR.N_ID_UTENTE
            and n_id_utente_udd = UDD.N_ID_UTENTE
        ) jj
    left outer join tariffa kk on jj.pod14 = kk.pod_tariffa
    left outer join configurazione KKK on jj.pod14 = kkK.pod_CONFIG
    left outer join residenza KKKK on jj.pod14 = kkKK.pod_residenza
    left outer join SW_storici_funzionali_T MMMM on jj.pod14 = MMMM.T_cODICE_POD
    ) ghgh
where 1=1
$(! [[ -z "${PIVA_DISTR_ARRAY[@]}" ]] && echo -n 'and PIVA_DISTR in ('\' && echo -n $(echo ${PIVA_DISTR_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_UDD_ARRAY[@]}" ]] && echo -n 'and PIVA_UDD in ('\' && echo -n $(echo ${PIVA_UDD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_COUPLES_ARRAY[@]}" ]] && echo -n 'and (' && echo -n "(PIVA_DISTR = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 1)' and PIVA_UDD = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 2)')" && for couple in "${PIVA_COUPLES_ARRAY[@]:1}"; do echo -n " or (PIVA_DISTR = '$(echo $couple | cut -d ',' -f 1)' and PIVA_UDD = '$(echo $couple | cut -d ',' -f 2)')"; done && echo -n ')')
END
)
export FUNZIONALI_1_ORACLE_CHECK_QUERY="select * from ${FUNZIONALI_1_ORACLE_TABLE_NAME} where rownum <= 1"
export FUNZIONALI_1_SPLIT_COLUMN="split_pod"
export FUNZIONALI_2_ORACLE_TABLE_NAME='SW_CON_TRA_VIEW_FU_NA_TB_TMP'
export FUNZIONALI_2_DELETE_ORACLE_QUERY="drop table ${FUNZIONALI_2_ORACLE_TABLE_NAME}"
export FUNZIONALI_2_ORACLE_QUERY=$(cat <<-END
create table ${FUNZIONALI_2_ORACLE_TABLE_NAME} COMPRESS FOR OLTP as
    with SW_storici_funzionali as (
            select /*+ PARALLEL (10) */ gg.*
                , to_number(REGEXP_REPLACE(pod14, '[^0-9]+', '')) as split_pod
            from (
                select to_char(trunc(D_dATA_PRESTAZIONE), 'yyyymm') ANNOMESE_SW
                    , T2.D_DATA_aPERTURA
                    , ud.t_codice_terna
                    , T1.N_ID_PRATICA
                    , SUBSTR(T_CODICE_POD, 1, 14) POD14
                    , TRUNC(D_DATA_PRESTAZIONE) D_DATA_aPERTURA_M
                    , to_char(N_ID_DISTR) N_ID_UTENTE_DISTR
                    , T_PROTOCOLLO
                    , to_char(AZ.N_ID_UTENTE) N_ID_UTENTE_UDD
                    , 1 att
                    , 1 atta
                    , to_char(D_dATA_PRESTAZIONE,'yyyy-mm-dd') as d_data_decorrenza
                from SWTCH.PRT_vSP T1
                join USERAPPL.T001_APP_PRT_PRATICHE T2 on T1.N_ID_PRATICA = T2.N_ID_PRATICA
                left join RCU.RCU_aZIENDA AZ on AZ.N_ID_AZIENDA = T1.N_ID_UTENTE
                left join RCU.RCU_UDD UD on UD.N_ID_UDD = AZ.N_ID_aZIENDA
                join SWTCH.prt_vsp_cod_prestazione CPT on CPT.T_CODICE = T_PRESTAZIONE
                    and CPT.T_TIPO = 'A'
                where 1=1
                    $(! [[ -z "${FUNZIONALI_NEW_ACTIVATION_ALL_MONTH}" ]] && echo -n 'and to_char(D_dATA_PRESTAZIONE,'\''yyyymm'\'') in ('\' && echo -n $(echo ${FUNZIONALI_NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${FUNZIONALI_NEW_ACTIVATION_DATE_ARRAY[@]}" ]] && echo -n 'and to_char(D_dATA_PRESTAZIONE,'\''yyyymmdd'\'') in ('\' && echo -n $(echo ${FUNZIONALI_NEW_ACTIVATION_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and SUBSTR(T_CODICE_POD, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    and T_ESITO_aGG_rCU = 'Y'

                union all

                select to_char(trunc(D_DATA_eSECUZIONE), 'yyyymm') ANNOMESE_SW
                    , T2.D_dATA_aPERTURA
                    , T1.T_COD_CONTR_DISP
                    , T1.N_ID_PRATICA
                    , SUBSTR(T1.T_cODICE_POD, 1, 14)
                    , TRUNC(D_DATA_eSECUZIONE) D_DATA_aPERTURA_M
                    , to_char(T1.N_ID_dISTR) as N_ID_UTENTE_DISTR
                    , T2.T_PROTOCOLLO
                    , to_char(T1.N_ID_CC) N_ID_UTENTE_UDD
                    , 0 DA_ATTIVARE_MESE_SUCC
                    , 0 ATTIVATI_NEL_MESE
                    , to_char(D_DATA_eSECUZIONE,'yyyy-mm-dd') as d_data_decorrenza
                from SWTCH.PRT_VS T1
                join USERAPPL.T001_APP_PRT_PRATICHE T2 on T1.N_ID_PRATICA = T2.N_ID_PRATICA
                    and T2.T_STATO != 'ANNULLATA'
                join SWTCH.PRT_vS_aGG_rCU T3 on T3.N_ID_PRATICA = T1.N_ID_PRATICA
                    and T_SERVIZIO_INSERIMENTO = 'VS1'
                    and T_ESITO_aGG_rCU = '1'
                join SWTCH.prt_vs_cod_prestazione T4 on T1.T_COD_PRESTAZIONE = T4.T_COD_PRESTAZIONE
                    and T4.T_DESCRIZIONE = 'Nuova Connessione con Attivazione'
                where 1=1
                    $(! [[ -z "${FUNZIONALI_NEW_ACTIVATION_ALL_MONTH}" ]] && echo -n 'and to_char(D_DATA_eSECUZIONE,'\''yyyymm'\'') in ('\' && echo -n $(echo ${FUNZIONALI_NEW_ACTIVATION_DATE_PARTITIONS_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${FUNZIONALI_NEW_ACTIVATION_DATE_ARRAY[@]}" ]] && echo -n 'and to_char(D_DATA_eSECUZIONE,'\''yyyymmdd'\'') in ('\' && echo -n $(echo ${FUNZIONALI_NEW_ACTIVATION_DATE_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and SUBSTR(T1.T_cODICE_POD, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                ) GG
            )
        , SW_storici_funzionali_T as (
            select /*+ PARALLEL (15) */ T_CODICE_POD
                , D_ANNO_MESE
                , TRATTAMENTO_ONLINE
            from (
                select X.*
                    , RANK() over (
                        partition by SUBSTR(T_CODICE_POD, 1, 14) order by COL asc
                        ) ranking
                from (
                    select 'O' COL
                        , X.*
                        , case
                            when d_Anno_mese = TRUNC(sysdate, 'MONTH')
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_ONLINE
                    from (
                        select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                            , X.N_ID_POD
                            , D_ANNO_MESE
                            , T_TRATTAMENTO
                            , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                            , X.N_CONSUMO_ANNUO
                            , X.T_NOTA
                            , X.D_AGGIORNAMENTO
                            , X.N_ID_TRACCIA
                            , X.N_ID_S_PREC
                        from rcu.rcu_pod_misure X
                            , RCU.RCU_POD POD
                            , SW_storici_funzionali ZZ
                        where SUBSTR(T_CODICE_POD, 1, 14) = ZZ.POD14
                            and d_Anno_mese <= TRUNC(sysdate, 'MONTH')
                            and X.N_ID_POD = POD.N_ID_POD
                        ) x
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14

                    union all

                    select 'S' COL
                        , T_CODICE_POD
                        , "N_ID_POD"
                        , MAX_D_ANNO_MESE
                        , "T_TRATTAMENTO"
                        , NVL("T_TRATTAMENTO_SUCC", T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                        , "N_CONSUMO_ANNUO"
                        , "T_NOTA"
                        , "MAX_D_AGGIORNAMENTO"
                        , "N_ID_TRACCIA"
                        , N_ID_S_PREC
                        , case
                            when d_Anno_mese = TRUNC(sysdate, 'MONTH')
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_STORICO
                    from (
                        select *
                        from (
                            select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                                , X.N_ID_POD
                                , D_ANNO_MESE
                                , T_TRATTAMENTO
                                , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                                , N_CONSUMO_ANNUO
                                , X.T_NOTA
                                , X.D_AGGIORNAMENTO
                                , X.N_ID_TRACCIA
                                , X.N_ID_S_PREC
                                , max(X.D_aGGIORNAMENTO) over (
                                    partition by X.N_ID_POD
                                    , D_ANNO_MESE
                                    ) MAX_D_AGGIORNAMENTO
                                , max(D_ANNO_MESE) over (partition by X.N_ID_POD) mAX_D_ANNO_MESE
                            from rcuS.rcuS_podmisure X
                                , RCU.RCU_POD POD
                                , SW_storici_funzionali ZZ
                            where d_Anno_mese <= TRUNC(sysdate, 'MONTH')
                                and X.N_ID_POD = POD.N_ID_POD
                                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                            )
                        where D_aGGIORNAMENTO = MAX_D_AGGIORNAMENTO
                            and D_ANNO_MESE = mAX_D_ANNO_MESE
                        ) X
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14
                    ) X
                )
            where RANKING = 1
            )
        , pod_tecn as (
            select zz.pod14 as pod
                , t_tipo_misuratore
                , pod.t_Area_Rif
                , n_Tensione
                , n_potenza_disponibile
                , n_potenza_impegnata
                , n_k_trasfor_Att
                , n_k_trasfor_rea
                , n_k_trasfor_pot
                , t_mat_misuratore_Att
                , t_mat_misuratore_rea
                , t_mat_misuratore_pot
                , d_inst_misurator_Att
                , d_inst_misurator_rea
                , d_inst_misurator_pot
                , n_num_cifre_att
                , n_num_cifre_rea
                , n_num_cifre_pot
                , B_PRESENZA_MIS
                , B_GEST_FORFAIT
                , d_oper_misurator_att as d_regime
            from rcu.rcu_pod_tecn x
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
            )
        , fornitura as (
            select zz.pod14 as pod_fornitura
                , x.t_diritto_tutela
                , x.b_disalimentabilita
            from rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
            )
        , tariffa as (
            select zz.pod14 as pod_tariffa
                , xx.t_tariffa_distr
            from rcu.rcu_TARIFFA xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.b_ultima = 'Y'
                and Xx.B_VALIDO = 'Y'
                and xx.n_id_fornitura = x.n_id_fornitura
            )
        , residenza as (
            select zz.pod14 as pod_residenza
                , xx.t_RESIDENTE
            from rcu.rcu_residenza xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , rcu.rcu_fornitura x
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.b_ultima = 'Y'
                and Xx.B_VALIDO = 'Y'
                and xx.n_id_fornitura = x.n_id_fornitura
            )
        , configurazione as (
            select distinct zz.pod14 as pod_config
                , case
                    when T_TIPO_CONFIGURAZIONE = 'C'
                        then 'SI'
                    else 'NO'
                    end T_TIPO_CONFIGURAZIONE
            from RCU.RCU_MISURATORE_2G xx
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
                , RCU.RCU_FASCE_MISURATORE_2G x
            where xx.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                and xx.d_fine_validita is null
                and xx.n_id_MISURATORE_2G = x.N_ID_MISURATORE
            )

select ANNOMESE_SW
    , D_DATA_APERTURA_M
    , t_codice_terna as T_COD_CONTR_DISP
    , N_ID_PRATICA
    , POD14
    , D_DATA_DECORRENZA
    , N_ID_UTENTE_DISTR
    , T_PROTOCOLLO
    , N_ID_UTENTE_UDD
    , att as DA_ATTIVARE_MESE_SUCC
    , atta as ATTIVATI_NEL_MESE
    , SPLIT_POD
    , T_CODICE_POD
    , D_ANNO_MESE
    , TRATTAMENTO_ONLINE
    , POD
    , T_TIPO_MISURATORE
    , T_AREA_RIF
    , N_TENSIONE
    , N_POTENZA_DISPONIBILE
    , N_POTENZA_IMPEGNATA
    , N_K_TRASFOR_ATT
    , N_K_TRASFOR_REA
    , N_K_TRASFOR_POT
    , T_MAT_MISURATORE_ATT
    , T_MAT_MISURATORE_REA
    , T_MAT_MISURATORE_POT
    , D_INST_MISURATOR_ATT
    , D_INST_MISURATOR_REA
    , D_INST_MISURATOR_POT
    , N_NUM_CIFRE_ATT
    , N_NUM_CIFRE_REA
    , N_NUM_CIFRE_POT
    , B_PRESENZA_MIS
    , B_GEST_FORFAIT
    , D_REGIME
    , POD_FORNITURA
    , T_DIRITTO_TUTELA
    , B_DISALIMENTABILITA
    , PIVA_DISTR
    , PIVA_UDD
    , T_RESIDENTE
    , T_TARIFFA_DISTR
    , T_TIPO_CONFIGURAZIONE
from (
    select jj.*
        , kkkk.t_RESIDENTE
        , kk.t_tariffa_distr
        , NVL(T_TIPO_CONFIGURAZIONE, 'NO') as T_TIPO_CONFIGURAZIONE
        , mmmm.*
    from (
        select X.*
            , K.*
            , KK.*
            , DISTR.T_PIVA as PIVA_DISTR
            , UDD.T_PIVA as PIVA_UDD
        from SW_storici_funzionali X
            , pod_tecn k
            , fornitura KK
            , RCU.RCU_AZIENDA DISTR
            , RCU.RCU_AZIENDA UDD
        where k.pod = x.pod14
            and KK.POD_fornitura = x.pod14
            and KK.POD_fornitura = x.pod14
            and n_id_utente_distr = DISTR.N_ID_UTENTE
            and n_id_utente_udd = UDD.N_ID_UTENTE
        ) jj
    left outer join tariffa kk on jj.pod14 = kk.pod_tariffa
    left outer join configurazione KKK on jj.pod14 = kkK.pod_CONFIG
    left outer join residenza KKKK on jj.pod14 = kkKK.pod_residenza
    left outer join SW_storici_funzionali_T MMMM on jj.pod14 = MMMM.T_cODICE_POD
    ) ghgh
where 1=1
$(! [[ -z "${PIVA_DISTR_ARRAY[@]}" ]] && echo -n 'and PIVA_DISTR in ('\' && echo -n $(echo ${PIVA_DISTR_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_UDD_ARRAY[@]}" ]] && echo -n 'and PIVA_UDD in ('\' && echo -n $(echo ${PIVA_UDD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_COUPLES_ARRAY[@]}" ]] && echo -n 'and (' && echo -n "(PIVA_DISTR = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 1)' and PIVA_UDD = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 2)')" && for couple in "${PIVA_COUPLES_ARRAY[@]:1}"; do echo -n " or (PIVA_DISTR = '$(echo $couple | cut -d ',' -f 1)' and PIVA_UDD = '$(echo $couple | cut -d ',' -f 2)')"; done && echo -n ')')
END
)
export FUNZIONALI_2_ORACLE_CHECK_QUERY="select * from ${FUNZIONALI_2_ORACLE_TABLE_NAME} where rownum <= 1"
export FUNZIONALI_2_SPLIT_COLUMN="split_pod"
export STORICI_1_ORACLE_TABLE_NAME='SW_CON_TRA_VIEW_RS_TB_TMP'
export STORICI_1_DELETE_ORACLE_QUERY="drop table ${STORICI_1_ORACLE_TABLE_NAME}"
export STORICI_1_ORACLE_QUERY=$(cat <<-END
create table ${STORICI_1_ORACLE_TABLE_NAME} COMPRESS FOR OLTP as
    with SW_storici_funzionali as (
            select /*+ PARALLEL (10) */ gg.*
                , to_number(REGEXP_REPLACE(pod14, '[^0-9]+', '')) as split_pod
            from (
                select to_char(trunc(t00.D_DATA_DECORRENZA), 'yyyymm') ANNOMESE_SW
                    , TRUNC(T01.D_DATA_aPERTURA, 'MONTH') D_DATA_aPERTURA_M
                    , t00.T_COD_CONTR_DISP
                    , t00.n_id_pratica
                    , substr(t00.t_codice_pod, 1, 14) as pod14
                    , to_char(t00.d_data_decorrenza,'yyyy-mm-dd')  as d_data_decorrenza
                    , to_char(t00.n_id_distr) as n_id_utente_distr
                    , t01.t_protocollo
                    , to_char(t01.n_id_utente) as n_id_utente_udd
                    , 1 as DA_ATTIVARE_MESE_SUCC
                    , 0 as ATTIVATI_NEL_MESE
                from swtch.prt_se T00
                join USERAPPL.T001_APP_PRT_PRATICHE T01 on T00.N_ID_PRATICA = T01.N_ID_PRATICA
                where 1=1
                    $(! [[ -z "${STORICI_SWITCHING_DATE}" ]] && echo -n "and to_char(t00.D_DATA_DECORRENZA,'yyyymmdd') = '${STORICI_SWITCHING_DATE}'")
                    $(! [[ -z "${STORICI_SWITCHING_DATE_ARRAY[@]}" ]] && echo -n "and to_char(t00.D_DATA_DECORRENZA,'yyyymm') in ('${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[0]}','${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[1]}')")
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and substr(t00.t_codice_pod, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    and b_ammissibile = 'Y'
                    and T01.T_STATO in ('IN CORSO','CHIUSA')
                    and T00.T_STATO not like '%E%'

                union all

                select to_char(trunc(t00.d_data_dec), 'yyyymm') ANNOMESE_SW
                    , TRUNC(T01.D_DATA_aPERTURA, 'MONTH') D_DATA_aPERTURA_M
                    , t00.T_COD_CONTR_DISP
                    , t00.n_id_pratica
                    , substr(t00.t_codice_pod, 1, 14) as pod14
                    , to_char(t00.d_data_dec,'yyyy-mm-dd') as d_data_decorrenza
                    , to_char(t00.n_id_distr) as n_id_utente_distr
                    , t01.t_protocollo
                    , to_char(t00.n_id_esercente) as n_id_utente_udd
                    , 1 as DA_ATTIVARE_MESE_SUCC
                    , 0 as ATTIVATI_NEL_MESE
                from swtch.prt_ui T00
                join USERAPPL.T001_APP_PRT_PRATICHE T01 on T00.N_ID_PRATICA = T01.N_ID_PRATICA
                where 1=1
                    $(! [[ -z "${STORICI_SWITCHING_DATE}" ]] && echo -n "and to_char(t00.d_data_dec,'yyyymmdd') = '${STORICI_SWITCHING_DATE}'")
                    $(! [[ -z "${STORICI_SWITCHING_DATE_ARRAY[@]}" ]] && echo -n "and to_char(t00.d_data_dec,'yyyymm') in ('${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[0]}','${STORICI_SWITCHING_DATE_PARTITIONS_ARRAY[1]}')")
                    $(! [[ -z "${POD_ARRAY[@]}" ]] && echo -n 'and substr(t00.t_codice_pod, 1, 14) in ('\' && echo -n $(echo ${POD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
                    and T01.T_STATO in ('IN CORSO','CHIUSA')
                    and T00.T_STATO not like '%E%'
                ) GG
            )
        , SW_storici_funzionali_T as (
            select /*+ PARALLEL (10) */ distinct T_CODICE_POD
                , D_ANNO_MESE
                , TRATTAMENTO_ONLINE
            from (
                select X.*
                    , RANK() over (
                        partition by SUBSTR(T_CODICE_POD, 1, 14) order by COL asc
                        ) ranking
                from (
                    select 'O' COL
                        , X.*
                        , case
                            when d_Anno_mese = ZZ.D_DATA_aPERTURA_M
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_ONLINE
                    from (
                        select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                            , X.N_ID_POD
                            , D_ANNO_MESE
                            , T_TRATTAMENTO
                            , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                            , X.N_CONSUMO_ANNUO
                            , X.T_NOTA
                            , X.D_AGGIORNAMENTO
                            , X.N_ID_TRACCIA
                            , X.N_ID_S_PREC
                        from rcu.rcu_pod_misure X
                            , RCU.RCU_POD POD
                            , SW_storici_funzionali ZZ
                        where SUBSTR(T_CODICE_POD, 1, 14) = ZZ.POD14
                            and d_Anno_mese <= ZZ.D_DATA_aPERTURA_M
                            and X.N_ID_POD = POD.N_ID_POD
                        ) x
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14

                    union all

                    select 'S' COL
                        , T_CODICE_POD
                        , "N_ID_POD"
                        , MAX_D_ANNO_MESE
                        , "T_TRATTAMENTO"
                        , NVL("T_TRATTAMENTO_SUCC", T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                        , "N_CONSUMO_ANNUO"
                        , "T_NOTA"
                        , "MAX_D_AGGIORNAMENTO"
                        , "N_ID_TRACCIA"
                        , N_ID_S_PREC
                        , case
                            when d_Anno_mese = ZZ.D_DATA_aPERTURA_M
                                then T_tRATTAMENTO
                            else T_TRATTAMENTO_SUCC
                            end TRATTAMENTO_STORICO
                    from (
                        select *
                        from (
                            select SUBSTR(T_CODICE_POD, 1, 14) T_CODICE_POD
                                , X.N_ID_POD
                                , D_ANNO_MESE
                                , T_TRATTAMENTO
                                , NVL(T_TRATTAMENTO_SUCC, T_tRATTAMENTO) T_TRATTAMENTO_SUCC
                                , N_CONSUMO_ANNUO
                                , X.T_NOTA
                                , X.D_AGGIORNAMENTO
                                , X.N_ID_TRACCIA
                                , X.N_ID_S_PREC
                                , max(X.D_aGGIORNAMENTO) over (
                                    partition by X.N_ID_POD
                                    , D_ANNO_MESE
                                    ) MAX_D_AGGIORNAMENTO
                                , max(D_ANNO_MESE) over (partition by X.N_ID_POD) mAX_D_ANNO_MESE
                            from rcuS.rcuS_podmisure X
                                , RCU.RCU_POD POD
                                , SW_storici_funzionali ZZ
                            where d_Anno_mese <= ZZ.D_DATA_aPERTURA_M
                                and X.N_ID_POD = POD.N_ID_POD
                                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
                            )
                        where D_aGGIORNAMENTO = MAX_D_AGGIORNAMENTO
                            and D_ANNO_MESE = mAX_D_ANNO_MESE
                        ) X
                        , SW_storici_funzionali ZZ
                    where T_CODICE_POD = ZZ.POD14
                    ) X
                )
            where RANKING = 1
            )
        , pod_tecn as (
            select distinct zz.pod14 as pod
                , replace(t_tipo_misuratore, 'O', 'E') as tipo_misuratore_last
            from rcu.rcu_pod_tecn x
                , rcu.rcu_pod pod
                , SW_storici_funzionali ZZ
            where x.n_id_pod = pod.n_id_pod
                and substr(T_CODICE_POD, 1, 14) = ZZ.POD14
            )

select annomese_sw
    , d_data_apertura_m
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
    , tipo_misuratore_last
    , piva_distr
    , piva_udd
from (
    select *
    from (
        select X.*
            , K.tipo_misuratore_last
            , DISTR.T_PIVA as PIVA_DISTR
            , UDD.T_PIVA as PIVA_UDD
        from SW_storici_funzionali X
            , RCU.RCU_AZIENDA DISTR
            , RCU.RCU_AZIENDA UDD
            , pod_tecn k
        where n_id_utente_distr = DISTR.N_ID_UTENTE
            and n_id_utente_udd = UDD.N_ID_UTENTE
            and k.pod = x.pod14
        ) X
    left outer join SW_storici_funzionali_T Y on X.POD14 = Y.T_cODICE_POD
    ) GHG
where 1=1
$(! [[ -z "${PIVA_DISTR_ARRAY[@]}" ]] && echo -n 'and piva_distr in ('\' && echo -n $(echo ${PIVA_DISTR_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_UDD_ARRAY[@]}" ]] && echo -n 'and piva_udd in ('\' && echo -n $(echo ${PIVA_UDD_ARRAY[@]} | sed "s/\ /\'\,\'/g") && echo -n \'')')
$(! [[ -z "${PIVA_COUPLES_ARRAY[@]}" ]] && echo -n 'and (' && echo -n "(piva_distr = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 1)' and piva_udd = '$(echo "${PIVA_COUPLES_ARRAY[0]}" | cut -d ',' -f 2)')" && for couple in "${PIVA_COUPLES_ARRAY[@]:1}"; do echo -n " or (piva_distr = '$(echo $couple | cut -d ',' -f 1)' and piva_udd = '$(echo $couple | cut -d ',' -f 2)')"; done && echo -n ')')
END
)
export STORICI_1_ORACLE_CHECK_QUERY="select * from ${STORICI_1_ORACLE_TABLE_NAME} where rownum <= 1"
export STORICI_1_SPLIT_COLUMN="split_pod"
