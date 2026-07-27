#!/bin/bash

F_LOG=$1

if [[ (! -v F_LOG) || -z "$F_LOG" ]];then
echo "Bisogna passare come parametro il file di log di scrittura!"
exit
fi

do_import () {

                eval tabella="$1"
                eval schema="$2"

                eval schema_nocorpo="$3"
                eval colonne_bfile="$4"


                echo "$(date) Scarico tabella ${tabella^^} in corso..."

                # Metto un 'a capo' tra uno scarico e l'altro
                echo ""

                # Avvio gli scarichi
                echo "$(date) Scarico tabella ${tabella^^} in corso..." &>> "$F_LOG" \
                && ( \
                        hdfs dfs -rm -R -f -skipTrash /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella}/ \
                        ; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m 50 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella} --direct \
                ) &> esito_sqoop_${tabella}.log \
                && ( \
                        ([ $(cat esito_sqoop_${tabella}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Importazione dati tabella ${tabella^^} completata con successo." > esito_sqoop_${tabella}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati tabella ${tabella} : " &>> esito_sqoop_${tabella}.log \
                ) \
                && ( \
                        rm -f ${tabella}.hql \
                        && echo "DROP TABLE IF EXISTS ${tabella}_new;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_new ( ${schema_nocorpo} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella}';" >> ${tabella}.hql \
			&& echo "DROP TABLE ${tabella}_p;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/${tabella}_p' AS SELECT *${colonne_bfile} FROM ${tabella}_new;" >> ${tabella}.hql \
                        && echo "DROP TABLE ${tabella}_new;" >> ${tabella}.hql \
                ) && ( \
                        hive -f ${tabella}.hql \
                        && rm -f ${tabella}.hql \
                ) &> esito_hive_${tabella}.log \
                && ( \
                        ([ $(cat esito_hive_${tabella}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Creazione tabella ${tabella^^}_p completata con successo." > esito_hive_${tabella}.log ) \
                        || echo "$(date) ERRORE nella creazione della tabella ${tabella^^}_p" &>> esito_hive_${tabella}.log \
                )
                cat esito_sqoop_${tabella}.log &>> "$F_LOG"
                cat esito_hive_${tabella}.log &>> "$F_LOG"

            rm -f esito_sqoop_${tabella}.log
            rm -f esito_hive_${tabella}.log

                rm -f "./"$tabella"_proc.logi"

                echo "$(date) Importazione tabella ${tabella^^} completato"  &>> "$F_LOG"
                echo "$(date) Importazione tabella ${tabella^^} completato"
}


tabelle_schemi=( \
        rcu.rcu_pod "N_ID_POD string, T_CODICE_POD string, T_AREA_RIF string, B_RICH_INDENNIZZO string, B_RICH_PREST_DISTR string, N_ID_INDIRIZZO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_IND_FORN string" \
        rcu.rcu_pod_distr "N_ID_POD string, N_ID_DISTR string, D_INIZIO string, D_FINE string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
        rcu.rcu_pod_misure "N_ID_POD string, D_ANNO_MESE string, T_TRATTAMENTO string, T_TRATTAMENTO_SUCC string, N_CONSUMO_ANNUO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
        rcu.rcu_pod_stato "N_ID_POD string, T_STATO_ATTIVAZIONE string, D_ATTIVAZIONE string, D_DISATTIVAZIONE string, T_CAUSALE_NO_RIATTIV string, T_CAUSALE_NO_DISATTIV string, T_STATO_SOSP string, D_SOSPENSIONE string, D_REVOCA_SOSP string, T_CAUSALE_NO_SOSP string, T_SWITCHING string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_COD_DISATTIVAZIONE string" \
        rcu.rcu_pod_udd "N_ID_POD string, N_ID_UDD string, D_INIZIO string, D_FINE string, D_STIPULA string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
)

#       tmpod.prt_tmo_mn "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, PUNTODISPACCIAMENTO string, DATAMISURA string, MOTIVAZIONE string, TRATTAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, D_RICEZIONE string, PERDITA string, ID_PROC string, GIORNOMISURA string, GRUPPOMIS string, FORFAIT string, KR string, KP string, MATRATT string, MATRREA string, MATRPOT string, DATAINSTMISATT string, DATAINSTMISREA string, DATAINSTMISPOT string, CIFREPOT string" \
#       tmpod.prt_tmo_mv "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, PIVA_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, DATAVOLTURA string, DATARILEVAZIONE string, TRATTAMENTO string, PUNTODISPACCIAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, CODPRATATT string, KR string, ERM string, ERF1 string, ERF2 string, ERF3 string" \
#       switch_gas.prt_vtg6 "N_ID_VTG6 string, N_ID_VTG string, N_ID_PRATICA string, N_ID_UTENTE string, T_CODICE_PDR string, T_MATR_MIS string, D_DATA_ATT_CONTR string, N_VOL_ANNUO_SOST string, T_CLASSE_GRUPPO_MIS string, T_CIFRE_MIS string, T_SEGN_MIS_SOST string, T_PRE_CONV string, T_GRUPPO_MIS_INT string, N_COEFF_CORR string, T_MATR_CONV string, T_CIFRE_CONV string, T_SEGN_CONV string, D_DATA_MIS_EFF string, T_SEGN_MIS_EFF string, T_SEGN_CONV_EFF string, T_NOTE string, T_TIPO_LETTURA string, B_COPIATO_TMG_MISURE string" \
#       cmg.prt_cmg_rml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, DATA_COMP string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, INI_PERIODO string, FINE_PERIODO string" \
#       cmg.prt_cmg_rmv "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, COD_PRAT_ATTIVAZIONE string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, PROGR_ANNO_TERM string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string" \
#       cmg.prt_cmg_rgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, PERIODO_RIC string" \
#       cmg.prt_cmg_tal "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
#       cmg.prt_cmg_tav "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
#       cmg.prt_cmg_tgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, VAL_DATO_MENS string, ESITO_RACCOLTA string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string" \
#       cmg.prt_cmg_tml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, ACC_MIS string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string, VAL_DATO string, NUM_TENTATIVI string, ESITO_RACCOLTA string, CAUSA_MANC_RACCOLTA string, MOD_ALT_RACC string, DIR_INDENNIZZO string, PROS_FIN string" \
#       rcugas.rcugas_connessioni_distr2 "T_CODICE_PDR string, N_ID_PDR string, N_ID_REMI string, D_DATA_INIZIO_CONN string, D_DATA_FINE_CONN string, T_REMI string, N_ID_DISTR string, D_DATA_INIZIO_GESTECN string, D_DATA_FINE_GESTECN string, T_REMI_RCU string, ID_REGIONE_CLIMATICA string" \
#       rcugas.rcugas_massivo "N_ID_PDR string, T_CODICE_PDR string, CAPACITA_TRASPORTO string, MESE_VAL_CAP_TRASP string, T_COD_TIPO_PDR string, T_DISALIMENTABILITA string, BILANCIAMENTO string, N_ID_FORNITURA string, D_DATA_INIZIO_FOR string, DATA_FINE_FOR string, N_ID_AZ_UDD string, PIVA_UDD string, N_ID_AZ_CC string, PIVA_CC string, N_ID_CLIENTE string, T_PARTITA_IVA_CLI string, T_CODICE_FISCALE_CLI string, B_CF_STRANIERO string, T_REFERENTE string, T_NOME_REF string, T_COGNOME_REF string, T_EMAIL_REF string, T_TELEFONO_REF string, T_RESIDENZA string, DATA_VAL_RES string, T_TOPONIMOPDR string, T_NOMESTRADA_PDR string, T_CIVICO_PDR string, T_CAP_PDR string, T_COMUNE_ISTAT_PDR string, T_COMUNE_PDR string, T_PROVINCIA_PDR string, T_NAZIONE_PDR string, ALTRO_IND_PDR string, T_TOPONIMO_FORN string, T_NOMESTRADA_FORN string, T_CIVICO_FORN string, T_CAP_FORN string, T_COMUNE_ISTATFORN string, T_COMUNE_FORN string, T_PROVINCIA_FORN string, T_NAZIONE_FORN string, ALTRO_IND_FORN string, T_ACCESSO_UI string, T_TIPO_FORNITURA string, T_ALIQUOTA_IVA string, T_ALIQUOTA_ACCISE string, T_ADD_REGIONALE string, T_ALTRE_INFO_IMPOSTE string, T_MATRICOLA_MISURATORE string, T_CLASSE_MISURATORE string, T_TIPO_MISURATORE string, T_TELEGESTIONE string, T_PRE_CONV string, T_MATRICOLA_CONVERTITORE string, N_NUM_CIFRE_CONVERTITORE string, T_ANNO_FABBRIC_CONVERTITORE string, T_DATA_INST_CONVERTITORE string, N_COEFF_CORREZIONE string, PRESS_MISURE string, T_ACCESS_MISURATORE string, N_NUM_CIFRE_MISURATORE string, T_ANNO_FABBRIC_MISURATORE string, T_DATA_INST_MISURATORE string, T_MISURATORE_INTEGRATO string, N_POTENZIALITA_MASSIMA string, N_POTENZIALITA_TOT_INSTALLATA string, N_MAX_PRELIEVO_ORARIO string, T_EROG_SERVIZIO_ENERG string, T_PARTITA_IVA_GESTCAL string, T_RAGIONE_SOCIALE_GESTCAL string, T_TELEFONO_GESTCAL string, T_EMAIL_GESTCAL string, T_TOPONIMO_GESTCAL string, T_NOMESTRADA_GESTCAL string, T_CIVICO_GESTCAL string, T_CAP_GESTCAL string, T_COMUNE_ISTAT_GESTCAL string, T_COMUNE_GESTCAL string, T_PROVINCIA_GESTCAL string, T_NAZIONE_GESTCAL string, T_INDIRIZZO_COMPLETO string, D_DATA_RIF_PDR string, D_AGGIORNAMENTO_PDR string, D_DATA_RIF_TECN string, D_AGGIORNAMENTO_TECN string, D_DATA_RIF_MIS string, D_AGGIORNAMENTO_MIS string, D_DATA_RIF_FORN string, D_AGGIORNAMENTO_FORN string, T_TIPO_BONUS string, D_DATA_INIZIO_EROG_BONUS string, D_DATA_FINE_EROG_BONUS string, D_DATA_RIF_BONUS string, D_AGGIORNAMENTO_BONUS string, D_DATA_AGGIORNAMENTO string, N_ID_UDD string, N_ID_VENDITORE string, T_COD_PROFILO string, T_COD_CAT_USO string, T_COD_CLASSE_PRELIEVO string, T_ANNO_TERMICO string, D_DATA_RIF_PREL string, T_TRATTAMENTO string, T_TOPONIMO_ESAZ string, T_NOMESTRADA_ESAZ string, T_CIVICO_ESAZ string, T_CAP_ESAZ string, T_COMUNE_ISTAT_ESAZ string, T_COMUNE_ESAZ string, T_PROVINCIA_ESAZ string, T_NAZIONE_ESAZ string, ALTRO_IND_ESAZ string, T_CODICE_ATECO string, T_PAGAMENTO_IVA string, T_CODICE_UFFICIO string, T_CF_INTESTATARIO_FATT string, T_CF_STRANIERO_FATT string, T_PIVA_INTESTATARIO_FATT string, T_NOME_INTESTATARIO_FATT string, T_COGNOME_INTESTATARIO_FATT string, T_RAG_SOC_INTESTATARIO_FATT string, T_ANNO_MESE_RINN_BONUS string, D_DATA_INIZIO_BONUS string, D_DATA_FINE_BONUS string, N_PRELIEVO_ANNUO string, T_FATTORE_CORREZ_CLIMATICA string, T_ALTRO_IND_GESTCAL string, T_TIPO_OP string, T_PROCESSO string,n_id_pratica_processo STRING" \
#       RCUGAS.RCUGAS_BILANCIAMENTO "N_ID_BILANCIAMENTO string, N_ID_UDB string, N_ID_PDR string, T_TIPO_BILANCIAMENTO string, D_DATA_INIZIO string, D_DATA_FINE string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string ,N_ID_S_PREC string,D_DATA_RIF string" \



for ((i =0; i < ${#tabelle_schemi[@]}; i++))
do
        if [ $((i%2)) -eq 0 ]
        then
                # | sed --expression='s/ string//g'
                tabella=${tabelle_schemi[$i]}
                schema=${tabelle_schemi[$i+1]}


                schema_nocorpo=$(echo ${schema} | sed --expression='s/T_CORPO[A-Z_]* string, //g')
                colonne_bfile=$(echo ${schema} | awk 'BEGIN {RS="string, "; ORS=""} /T_CORPO[A-Z_]*/ {print ",'\'\'' AS " $0}')



                rm -f "./"$tabella"_proc.logi"
                echo "in_process" > "./"$tabella"_proc.logi"

                if [[ $tabella == "rcu.rcu_pod_tecn" ]]
                then
                   do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}"
                else
                   do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}" &
                fi


        fi

done


viste_schemi=( \
        rcu.v_rcu_azienda "N_ID_AZIENDA string, N_ID_UTENTE string, T_CODICE_AEEG string, T_PIVA string, T_CF string, T_RAG_SOC string, N_ID_SEDELEGALE string, T_CONTATTO string, T_EMAIL string, T_PEC string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_RUOLI string, T_DESC_RUOLI string" \
        rcugas.v_rcugas_distributore "N_ID_DISTRIBUTORE string, N_ID_AZIENDA string, T_CODICE_ESERCENTE string, N_ID_UTENTE string, T_CODICE_AEEG string, T_PIVA string, T_RAG_SOC string, D_DATA_INIZIO string, D_DATA_FINE string" \
        tmpod_cloud.DISTR_AZ "N_ID_DISTR STRING,N_ID_DISTR_RIF STRING,T_TIPO STRING,T_PIVA STRING" \
        rcu.rcu_udd "N_ID_UDD string, T_CODICE_TERNA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_INIZIO string, D_FINE string, N_ID_AZIENDA_RIF string, T_TIPO string" \
)


for ((i = 0; i < ${#viste_schemi[@]}; i++))
do
        if [ $((i%2)) -eq 0 ]
        then
                vista=${viste_schemi[$i]}
                schema=${viste_schemi[$i+1]}

                echo ""
                echo "$(date) Scarico vista ${vista^^} in corso..."

                echo "$(date) Scarico vista ${vista^^} in corso..." &>> "$F_LOG" \
                && ( \
                        hdfs dfs -rm -R -f -skipTrash /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${vista}/ \
                        ; sqoop import --connect jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${vista^^} -m 1 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${vista} --direct \
                ) &> esito_sqoop_${vista}.log \
                && ( \
                        ([ $(cat esito_sqoop_${vista}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Importazione dati vista ${vista^^} completata con successo." > esito_sqoop_${vista}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati vista ${vista} : " &>> esito_sqoop_${vista}.log \
                ) \
                && ( \
                        rm -f ${vista}.hql \
                        && echo "DROP TABLE IF EXISTS ${vista}_new;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_new ( ${schema} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/n${vista}';" >> ${vista}.hql \
			&& echo "DROP TABLE ${vista}_p;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/${vista}_p' AS SELECT * FROM ${vista}_new;" >> ${vista}.hql \
                        && echo "DROP TABLE ${vista}_new;" >> ${vista}.hql \
                ) && ( \
                        hive -f ${vista}.hql \
                        && rm -f ${vista}.hql \
                ) &> esito_hive_${vista}.log \
                && ( \
                        ([ $(cat esito_hive_${vista}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Creazione vista  ${vista^^}_p completata con successo." > esito_hive_${vista}.log ) \
                        || echo "$(date) ERRORE nella creazione della vista ${vista^^}_p " &>> esito_hive_${vista}.log \
                )

                cat esito_sqoop_${vista}.log &>> "$F_LOG"
                cat esito_hive_${vista}.log &>> "$F_LOG"

                rm -f esito_sqoop_${vista}.log
                rm -f esito_hive_${vista}.log

        fi
done

in_import=true

while $in_import; do


sleep 3

num_import=$(ls -all | grep ".logi" | wc -l)


if [ $((num_import)) -eq 0 ];then in_import=false; fi

done



echo ""
echo "$(date) Tutte le operazioni sono concluse"
echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"
