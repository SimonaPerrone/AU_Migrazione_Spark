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
        rcu.rcu_azienda "N_ID_AZIENDA string, N_ID_UTENTE string, T_CODICE_AEEG string, T_PIVA string, T_CF string, T_RAG_SOC string, N_ID_SEDELEGALE string, T_CONTATTO string, T_EMAIL string, T_PEC string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
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


in_import=true

while $in_import; do


sleep 3

num_import=$(ls -all | grep ".logi" | wc -l)


if [ $((num_import)) -eq 0 ];then in_import=false; fi

done



echo ""
echo "$(date) Tutte le operazioni sono concluse"
echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"

