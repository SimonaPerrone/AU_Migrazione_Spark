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
       tmpod.prt_tmo_mn "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, PUNTODISPACCIAMENTO string, DATAMISURA string, MOTIVAZIONE string, TRATTAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, D_RICEZIONE string, PERDITA string, ID_PROC string, GIORNOMISURA string, GRUPPOMIS string, FORFAIT string, KR string, KP string, MATRATT string, MATRREA string, MATRPOT string, DATAINSTMISATT string, DATAINSTMISREA string, DATAINSTMISPOT string, CIFREPOT string" \
       tmpod.prt_tmo_mv "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, PIVA_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, DATAVOLTURA string, DATARILEVAZIONE string, TRATTAMENTO string, PUNTODISPACCIAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, CODPRATATT string, KR string, ERM string, ERF1 string, ERF2 string, ERF3 string" \
)

#       tmpod.prt_tmo_mn "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, PUNTODISPACCIAMENTO string, DATAMISURA string, MOTIVAZIONE string, TRATTAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, D_RICEZIONE string, PERDITA string, ID_PROC string, GIORNOMISURA string, GRUPPOMIS string, FORFAIT string, KR string, KP string, MATRATT string, MATRREA string, MATRPOT string, DATAINSTMISATT string, DATAINSTMISREA string, DATAINSTMISPOT string, CIFREPOT string" \
#       tmpod.prt_tmo_mv "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, PIVA_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, DATAVOLTURA string, DATARILEVAZIONE string, TRATTAMENTO string, PUNTODISPACCIAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, CODPRATATT string, KR string, ERM string, ERF1 string, ERF2 string, ERF3 string" \
#       switch_gas.prt_vtg6 "N_ID_VTG6 string, N_ID_VTG string, N_ID_PRATICA string, N_ID_UTENTE string, T_CODICE_PDR string, T_MATR_MIS string, D_DATA_ATT_CONTR string, N_VOL_ANNUO_SOST string, T_CLASSE_GRUPPO_MIS string, T_CIFRE_MIS string, T_SEGN_MIS_SOST string, T_PRE_CONV string, T_GRUPPO_MIS_INT string, N_COEFF_CORR string, T_MATR_CONV string, T_CIFRE_CONV string, T_SEGN_CONV string, D_DATA_MIS_EFF string, T_SEGN_MIS_EFF string, T_SEGN_CONV_EFF string, T_NOTE string, T_TIPO_LETTURA string, B_COPIATO_TMG_MISURE string" \
#	cmg.prt_cmg_rml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, DATA_COMP string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, INI_PERIODO string, FINE_PERIODO string" \
#	cmg.prt_cmg_rmv "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, COD_PRAT_ATTIVAZIONE string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, PROGR_ANNO_TERM string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string" \
#	cmg.prt_cmg_rgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, MOT_RETT_LETT string, VOL_RIC string, PERIODO_RIC string" \
#	cmg.prt_cmg_tal "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
#	cmg.prt_cmg_tav "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, DATA_COM_AUTOLET_CF string, LET_TOT_PREL string, LET_TOT_CONV string, ESITO_VAL string, NOTE string" \
#	cmg.prt_cmg_tgl "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, MESE_COMP string, COD_PDR string, MATR_MIS string, MATR_CONV string, VAL_DATO_MENS string, ESITO_RACCOLTA string, DATA_COMP string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string" \
#	cmg.prt_cmg_tml "N_ID string, N_ID_FILE string, ANNOMESE_RIFERIMENTO string, DT_CARICAMENTO string, COD_SERVIZIO string, COD_FLUSSO string, PIVA_UTENTE string, PIVA_DISTR string, COD_PDR string, MATR_MIS string, MATR_CONV string, COEFF_CORR string, FREQ_LET string, ACC_MIS string, DATA_RACC string, LET_TOT_PREL string, LET_TOT_CONV string, TIPO_LETTURA string, VAL_DATO string, NUM_TENTATIVI string, ESITO_RACCOLTA string, CAUSA_MANC_RACCOLTA string, MOD_ALT_RACC string, DIR_INDENNIZZO string, PROS_FIN string" \
	


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



in_import=true

while $in_import; do


sleep 3

num_import=$(ls -all | grep ".logi" | wc -l)


if [ $((num_import)) -eq 0 ];then in_import=false; fi

done



echo ""
echo "$(date) Tutte le operazioni sono concluse"
echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"

