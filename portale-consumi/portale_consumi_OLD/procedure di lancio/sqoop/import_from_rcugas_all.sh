#!/bin/bash

F_LOG=$1



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
                        hdfs dfs -rm -R -f -skipTrash /user/hive/warehouse/acquirente_unico/sqoop/n${tabella}/ \
                        ; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m 30 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /user/hive/warehouse/acquirente_unico/sqoop/n${tabella} --direct \
                ) &> esito_sqoop_${tabella}.log \
                && ( \
                        ([ $(cat esito_sqoop_${tabella}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Importazione dati tabella ${tabella^^} completata con successo." > esito_sqoop_${tabella}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati tabella ${tabella} : " &>> esito_sqoop_${tabella}.log \
                ) \
                && ( \
                        rm -f ${tabella}.hql \
                        && echo "DROP TABLE IF EXISTS ${tabella}_new;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_new ( ${schema_nocorpo} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/acquirente_unico/sqoop/n${tabella}';" >> ${tabella}.hql \
                        && echo "DROP TABLE ${tabella}_p;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop/${tabella}_p' AS SELECT *${colonne_bfile} FROM ${tabella}_new;" >> ${tabella}.hql \
                        && echo "DROP TABLE ${tabella}_new;" >> ${tabella}.hql \
                ) && ( \
                        hive -f ${tabella}.hql \
                        && rm -f ${tabella}.hql \
                ) &> esito_hive_${tabella}.log \
                && ( \
                        ([ $(cat esito_hive_${tabella}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Creazione tabella ${tabella^^}_p completata con successo." > esito_hive_${tabella}.log ) \
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
        rcugas.rcugas_clientefinale "N_ID_CLIENTE string, T_CODICE_FISCALE string, T_PARTITA_IVA string, T_NOME string, T_COGNOME string, T_RAGIONE_SOCIALE string, T_NOTE string, T_DETTAGLIO_CF string, T_DETTAGLIO_PIVA string, T_SEDE_LEGALE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_DETTAGLIO_ANACLI string, D_DATA_RIF string, T_CODICE_ATECO string, B_CF_STRANIERO string, B_PERSONA_FISICA string, T_TELEFONO string, T_EMAIL string" \
        rcugas.rcugas_connessioni_distr "T_CODICE_PDR string, N_ID_PDR string, N_ID_REMI string, D_DATA_INIZIO_CONN string, D_DATA_FINE_CONN string, T_REMI string, N_ID_DISTR string, D_DATA_INIZIO_GESTECN string, D_DATA_FINE_GESTECN string" \
        rcugas.rcugas_connessioni_distr2 "T_CODICE_PDR string, N_ID_PDR string, N_ID_REMI string, D_DATA_INIZIO_CONN string, D_DATA_FINE_CONN string, T_REMI string, N_ID_DISTR string, D_DATA_INIZIO_GESTECN string, D_DATA_FINE_GESTECN string, T_REMI_RCU string, ID_REGIONE_CLIMATICA string" \
        rcugas.rcugas_distributore "N_ID_DISTRIBUTORE string, N_ID_AZIENDA string, T_CODICE_ESERCENTE string, COD_TIPO_DISTRIBUTORE string, D_DATA_INIZIO string, D_DATA_FINE string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string" \
        rcugas.rcugas_fornitura "N_ID_FORNITURA string, D_DATA_INIZIO string, D_DATA_FINE string, N_ID_CLIENTE string, N_ID_PDR string, N_ID_VEND string, B_TARIFFA_TM string, T_CODICE_ATECO string, N_LETTURA_ATTIVAZIONE string, T_ALIQUOTA_IVA string, T_IMPOSTE string, N_INDIRIZZO_FORNITURA string, N_INDIRIZZO_RECAP string, T_BONUS_GAS string, D_DATA_INIZIO_BONUS string, D_DATA_FINE_BONUS string, B_PRESTAZIONI_NON_CONCLUSE string, B_DISALIMENTABILITA string, T_CODICE_CONTRATTO_VENDITA string, T_ID_CONTRATTO_VEND string, D_DATA_STIPULA string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, TIPO_DATA_INIZIO string, TIPO_DATA_FINE string, D_DATA_RIF string, T_TIPO_FORNITURA string, N_INDIRIZZO_FATT string" \
        rcugas.rcugas_indirizzi "N_ID string, T_TOPONIMO string, T_NOMESTRADA string, T_CIVICO string, T_COMUNE string, T_COMUNE_ISTAT string, T_PROVINCIA string, T_NAZIONE string, T_INDIRIZZO_COMPLETO string, T_PRESSO string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string, T_CAP string" \
        rcugas.rcugas_massivo "N_ID_PDR string, T_CODICE_PDR string, CAPACITA_TRASPORTO string, MESE_VAL_CAP_TRASP string, T_COD_TIPO_PDR string, T_DISALIMENTABILITA string, BILANCIAMENTO string, N_ID_FORNITURA string, D_DATA_INIZIO_FOR string, DATA_FINE_FOR string, N_ID_AZ_UDD string, PIVA_UDD string, N_ID_AZ_CC string, PIVA_CC string, N_ID_CLIENTE string, T_PARTITA_IVA_CLI string, T_CODICE_FISCALE_CLI string, B_CF_STRANIERO string, T_REFERENTE string, T_NOME_REF string, T_COGNOME_REF string, T_EMAIL_REF string, T_TELEFONO_REF string, T_RESIDENZA string, DATA_VAL_RES string, T_TOPONIMOPDR string, T_NOMESTRADA_PDR string, T_CIVICO_PDR string, T_CAP_PDR string, T_COMUNE_ISTAT_PDR string, T_COMUNE_PDR string, T_PROVINCIA_PDR string, T_NAZIONE_PDR string, ALTRO_IND_PDR string, T_TOPONIMO_FORN string, T_NOMESTRADA_FORN string, T_CIVICO_FORN string, T_CAP_FORN string, T_COMUNE_ISTATFORN string, T_COMUNE_FORN string, T_PROVINCIA_FORN string, T_NAZIONE_FORN string, ALTRO_IND_FORN string, T_ACCESSO_UI string, T_TIPO_FORNITURA string, T_ALIQUOTA_IVA string, T_ALIQUOTA_ACCISE string, T_ADD_REGIONALE string, T_ALTRE_INFO_IMPOSTE string, T_MATRICOLA_MISURATORE string, T_CLASSE_MISURATORE string, T_TIPO_MISURATORE string, T_TELEGESTIONE string, T_PRE_CONV string, T_MATRICOLA_CONVERTITORE string, N_NUM_CIFRE_CONVERTITORE string, T_ANNO_FABBRIC_CONVERTITORE string, T_DATA_INST_CONVERTITORE string, N_COEFF_CORREZIONE string, PRESS_MISURE string, T_ACCESS_MISURATORE string, N_NUM_CIFRE_MISURATORE string, T_ANNO_FABBRIC_MISURATORE string, T_DATA_INST_MISURATORE string, T_MISURATORE_INTEGRATO string, N_POTENZIALITA_MASSIMA string, N_POTENZIALITA_TOT_INSTALLATA string, N_MAX_PRELIEVO_ORARIO string, T_EROG_SERVIZIO_ENERG string, T_PARTITA_IVA_GESTCAL string, T_RAGIONE_SOCIALE_GESTCAL string, T_TELEFONO_GESTCAL string, T_EMAIL_GESTCAL string, T_TOPONIMO_GESTCAL string, T_NOMESTRADA_GESTCAL string, T_CIVICO_GESTCAL string, T_CAP_GESTCAL string, T_COMUNE_ISTAT_GESTCAL string, T_COMUNE_GESTCAL string, T_PROVINCIA_GESTCAL string, T_NAZIONE_GESTCAL string, T_INDIRIZZO_COMPLETO string, D_DATA_RIF_PDR string, D_AGGIORNAMENTO_PDR string, D_DATA_RIF_TECN string, D_AGGIORNAMENTO_TECN string, D_DATA_RIF_MIS string, D_AGGIORNAMENTO_MIS string, D_DATA_RIF_FORN string, D_AGGIORNAMENTO_FORN string, T_TIPO_BONUS string, D_DATA_INIZIO_EROG_BONUS string, D_DATA_FINE_EROG_BONUS string, D_DATA_RIF_BONUS string, D_AGGIORNAMENTO_BONUS string, D_DATA_AGGIORNAMENTO string, N_ID_UDD string, N_ID_VENDITORE string, T_COD_PROFILO string, T_COD_CAT_USO string, T_COD_CLASSE_PRELIEVO string, T_ANNO_TERMICO string, D_DATA_RIF_PREL string, T_TRATTAMENTO string, T_TOPONIMO_ESAZ string, T_NOMESTRADA_ESAZ string, T_CIVICO_ESAZ string, T_CAP_ESAZ string, T_COMUNE_ISTAT_ESAZ string, T_COMUNE_ESAZ string, T_PROVINCIA_ESAZ string, T_NAZIONE_ESAZ string, ALTRO_IND_ESAZ string, T_CODICE_ATECO string, T_PAGAMENTO_IVA string, T_CODICE_UFFICIO string, T_CF_INTESTATARIO_FATT string, T_CF_STRANIERO_FATT string, T_PIVA_INTESTATARIO_FATT string, T_NOME_INTESTATARIO_FATT string, T_COGNOME_INTESTATARIO_FATT string, T_RAG_SOC_INTESTATARIO_FATT string, T_ANNO_MESE_RINN_BONUS string, D_DATA_INIZIO_BONUS string, D_DATA_FINE_BONUS string, N_PRELIEVO_ANNUO string, T_FATTORE_CORREZ_CLIMATICA string, T_ALTRO_IND_GESTCAL string, T_TIPO_OP string, T_PROCESSO string,n_id_pratica_processo STRING" \
        rcugas.rcugas_pdr "N_ID_PDR string, T_CODICE_PDR string, T_COD_TIPO_PDR string, T_CODICE_ISTAT string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_INDIRIZZO string, D_DATA_RIF string, T_DISALIMENTABILITA string, T_ACCESSO_UI string" \
        rcugas.rcugas_pdr_datiprelievo "N_ID_PDR_DATIPRELIEVO string, N_ID_PDR string, T_ANNO string, T_COD_PROFILO string, N_PRELIEVO_ANNUO string, N_LETTURA_CONVERTITORE string, T_COD_CAT_USO string, T_COD_CLASSE_PRELIEVO string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string, T_ANNO_MESE_RIF string, T_FATTORE_CORREZ_CLIMATICA string, T_TRATTAMENTO_SETTLEMENT string" \
        rcugas.rcugas_pdr_misuratore "N_ID_PDR_MISURATORE string, N_ID_PDR string, T_MATRICOLA_MISURATORE string, T_TIPO_MISURATORE string, T_TELEGESTITO string, N_COEFF_CORREZIONE string, T_CLASSE_MISURATORE string, T_ACCESS_MISURATORE string, N_NUM_CIFRE_MISURATORE string, T_ANNO_FABBRIC_MISURATORE string, T_DATA_INST_MISURATORE string, T_MISURATORE_INTEGRATO string, T_PRESENZA_CONVERTITORE string, T_MATRICOLA_CONVERTITORE string, N_NUM_CIFRE_CONVERTITORE string, T_ANNO_FABBRIC_CONVERTITORE string, T_DATA_INST_CONVERTITORE string, N_LETTURA_CONVERTITORE string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string" \
        rcugas.rcugas_residenza "N_ID_RESIDENZA string, N_ID_FORNITURA string, T_RESIDENZA string, D_DATA_INIZIO string, D_DATA_FINE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string" \
        rcugas.rcugas_venditore "N_ID_VENDITORE string, N_ID_AZIENDA string, T_CODICE_MAP string, D_DATA_INIZIO string, D_DATA_FINE string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string" \
        rcus.rcus_fornitura "N_ID_SCHEDA string, N_ID_FORNITURA string, N_ID_POD string, N_ID_CLIENTE string, D_INIZIO_TITOLARITA string, D_FINE_TITOLARITA string, T_COD_CONTRATTO string, D_STIPULA_CONTRATTO string, D_MESE_RINNOVO string, N_IVA string, B_DISALIMENTABILITA string, T_TARIFFA_DISTR string, T_CODICE_ATECO string, N_ID_FORNITORE string, T_RUOLO_FORNITORE string, T_TIPO_MERCATO string, B_SALVAGUARDIA string, T_BONUS_SOCIALE string, D_INIZIO_BONUS string, D_FINE_BONUS string, T_COMUNIC_BONUS string, N_IMPOSTE string, N_ID_INDIR_ESAZIONE string, N_ID_INDIR_COMUNIC string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string, T_DIRITTO_TUTELA string, T_CODICE_UFFICIO string, T_PAGAMENTO_IVA string, T_ADDIZ_PROVINCIALE string, T_ADDIZ_COMUNALE string, T_TELEFONO string, T_IVA string, T_IMPOSTE string" \
        RCUGAS.RCUGAS_BILANCIAMENTO "N_ID_BILANCIAMENTO string, N_ID_UDB string, N_ID_PDR string, T_TIPO_BILANCIAMENTO string, D_DATA_INIZIO string, D_DATA_FINE string, T_NOTE string, D_AGGIORNAMENTO string, N_ID_TRACCIA string ,N_ID_S_PREC string,D_DATA_RIF string" \
)

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

                if [[ $tabella == "rcu.rcu_pod_tecn_old" ]]
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
                        hdfs dfs -rm -R -f -skipTrash /user/hive/warehouse/acquirente_unico/sqoop/n${vista}/ \
                        ; sqoop import --connect jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${vista^^} -m 1 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /user/hive/warehouse/acquirente_unico/sqoop/n${vista} --direct \
                ) &> esito_sqoop_${vista}.log \
                && ( \
                        ([ $(cat esito_sqoop_${vista}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Importazione dati vista ${vista^^} completata con successo." > esito_sqoop_${vista}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati vista ${vista} : " &>> esito_sqoop_${vista}.log \
                ) \
                && ( \
                        rm -f ${vista}.hql \
                        && echo "DROP TABLE IF EXISTS ${vista}_new;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_new ( ${schema} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/acquirente_unico/sqoop/n${vista}';" >> ${vista}.hql \
                        && echo "DROP TABLE ${vista}_p;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop/${vista}_p' AS SELECT * FROM ${vista}_new;" >> ${vista}.hql \
                        && echo "DROP TABLE ${vista}_new;" >> ${vista}.hql \
                ) && ( \
                        hive -f ${vista}.hql \
                        && rm -f ${vista}.hql \
                ) &> esito_hive_${vista}.log \
                && ( \
                        ([ $(cat esito_hive_${vista}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Creazione vista  ${vista^^}_p completata con successo." > esito_hive_${vista}.log ) \
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

