#!/bin/bash
CURTMS=`date +%Y%m%d%H%M%S`
F_LOG="/home/leonardo/portale_consumi/logs/log_CMG_$CURTMS""_GAS.txt"



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
                        hdfs dfs -rm -R -f -skipTrash /acquirente_unico/n${tabella}/ \
                        ; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m 30 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /acquirente_unico/n${tabella} --direct \
                ) &> esito_sqoop_${tabella}.log \
                && ( \
                        ([ $(cat esito_sqoop_${tabella}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Importazione dati tabella ${tabella^^} completata con successo." > esito_sqoop_${tabella}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati tabella ${tabella} : " &>> esito_sqoop_${tabella}.log \
                ) \
                && ( \
                        rm -f ${tabella}.hql \
                        && echo "DROP TABLE IF EXISTS ${tabella}_new;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_new ( ${schema_nocorpo} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/acquirente_unico/n${tabella}';" >> ${tabella}.hql \
                        && echo "DROP TABLE ${tabella}_p;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_p STORED AS PARQUET LOCATION '/acquirente_unico/${tabella}_p' AS SELECT *${colonne_bfile} FROM ${tabella}_new;" >> ${tabella}.hql \
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
        cmg.prt_cmg "N_ID_CMG string, N_ID_PRATICA string, T_TIPO string, T_STATO string, D_DATA_RICH string, N_ID_DESTINATARIO string, T_RUOLO_DESTINATARIO string, T_PIVA_DESTINATARIO string, T_RAG_SOC_DESTINATARIO string, T_ANNO string, T_MESE string, B_AMMISSIBILE string, T_COD_CAUSALE string, T_MOTIVAZIONE string, D_DATA_STATO string, D_DATA_DOWNLOAD string" \
        cmg.prt_cmg_def "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PRAT_UTENTE string, COD_PRAT_DISTR string, COD_PDR string, MATR_MIS string, DATA_DECO_SWITCH string, VOL_ANNUO_SOST string, CLASSE_GRUPPO_MIS string, N_CIFRE_MIS string, SEGN_MIS_SOST string, TIPO_LETTURA string, PRE_CONV string, GRUPPO_MIS_INT string, COEFF_CORR string, MATR_CONV string, N_CIFRE_CONV string, SEGN_CONV string, DATA_MIS_EFF string, SEGN_MIS_EFF string, SEGN_CONV_EFF string, NOTE string" \
        cmg.prt_cmg_file "N_ID_FILE string, N_ID_CMG string, T_NOME_FILE string, T_TIPO_FILE string, T_STATO_FILE string, D_DATA_CARICAMENTO string, T_ANNO_CARICAMENTO string, T_MESE_CARICAMENTO string, T_GIORNO_CARICAMENTO string, B_AMMISSIBILE string, T_COD_CAUSALE string, T_MOTIVAZIONE string, T_CORPO string, T_DIGEST string, N_DIMENSIONE string, B_INVIATO string, T_PIVA_DISTRIBUTORE string, T_PIVA_UDD string, T_TIPO_SERVIZIO string, T_TIPO_FLUSSO string, PRESENTE_DB string, T_ANNOMESE_RIF string, T_CORPO_CSV string, N_ID_PADRE string, B_VERIFICATO string" \
        cmg.prt_cmg_fui "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PRAT_UTENTE string, COD_PRAT_DISTR string, COD_PDR string, MATR_MIS string, DATA_DECO_SWITCH string, VOL_ANNUO_SOST string, CLASSE_GRUPPO_MIS string, N_CIFRE_MIS string, SEGN_MIS_SOST string, TIPO_LETTURA string, PRE_CONV string, GRUPPO_MIS_INT string, COEFF_CORR string, MATR_CONV string, N_CIFRE_CONV string, SEGN_CONV string, DATA_MIS_EFF string, SEGN_MIS_EFF string, SEGN_CONV_EFF string, NOTE string" \
        cmg.prt_cmg_rgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, PERIODO_RIC string" \
        cmg.prt_cmg_rml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, DATA_COMP string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, INI_PERIODO string, FINE_PERIODO string" \
        cmg.prt_cmg_rmv "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, COD_PRAT_ATTIVAZIONE string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, PROGR_ANNO_TERM string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string" \
        cmg.prt_cmg_rsl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, PROGR_ANNO_TERM string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string" \
        cmg.prt_cmg_sw1 "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PRAT_UTENTE string, COD_PRAT_DISTR string, COD_PDR string, MATR_MIS string, DATA_DECO_SWITCH string, VOL_ANNUO_SOST string, CLASSE_GRUPPO_MIS string, N_CIFRE_MIS string, SEGN_MIS_SOST string, TIPO_LETTURA string, PRE_CONV string, GRUPPO_MIS_INT string, COEFF_CORR string, MATR_CONV string, N_CIFRE_CONV string, SEGN_CONV string, DATA_MIS_EFF string, SEGN_MIS_EFF string, SEGN_CONV_EFF string, NOTE string" \
        cmg.prt_cmg_tal "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
        cmg.prt_cmg_tav "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
        cmg.prt_cmg_tgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, VAL_DATO_MENS string, ESITO_RACCOLTA string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string" \
        cmg.prt_cmg_tml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, ACC_MIS string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string, VAL_DATO string, NUM_TENTATIVI string, ESITO_RACCOLTA string, CAUSA_MANC_RACCOLTA string, MOD_ALT_RACC string, DIR_INDENNIZZO string, PROS_FIN string" \
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

                do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}" 


        fi

done

echo ""
echo "$(date) Tutte le operazioni sono concluse"
echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"


