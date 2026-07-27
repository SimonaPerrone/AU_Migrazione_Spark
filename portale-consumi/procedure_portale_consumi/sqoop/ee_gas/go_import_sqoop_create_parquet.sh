#!/bin/bash

do_import () {
                eval F_LOG="$1"
                eval tabella="$2"
                eval num_mappers="$3"

                eval schema_nocorpo="$4"
                eval colonne_bfile="$5"
                eval tabella_output="$6"

                #echo "TABELLA=$tabella TABELLA_OUTPUT=$tabella_output SCHEMA=$schema_nocorpo COLONNE=$colonne_bfile MAPS=$num_mappers"

                echo "$(date) Scarico tabella ${tabella^^} in corso..."

                # Metto un 'a capo' tra uno scarico e l'altro
                echo ""

                # Avvio gli scarichi
                echo "$(date) Scarico tabella ${tabella^^} in corso..." &>> "$F_LOG" \
                && ( \
                        hdfs dfs -rm -R -f -skipTrash /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella_output}/ \
                        ; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m ${num_mappers} --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella_output} --direct \
                ) &> esito_sqoop_${tabella}.log \
                && ( \
                        ([ $(cat esito_sqoop_${tabella}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Importazione dati tabella ${tabella^^} completata con successo." &>> esito_sqoop_${tabella}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati tabella ${tabella} : " &>> esito_sqoop_${tabella}.log \
                ) \
                && ( \
                        rm -f ${tabella_output}.hql \
                        && echo "DROP TABLE IF EXISTS ${tabella_output}_new;" >> ${tabella_output}.hql \
                        && echo "CREATE TABLE ${tabella_output}_new ( ${schema_nocorpo} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/n${tabella_output}';" >> ${tabella_output}.hql \
                        && echo "DROP TABLE ${tabella_output}_p;" >> ${tabella_output}.hql \
                        && echo "CREATE TABLE ${tabella_output}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/${tabella}_p' AS SELECT *${colonne_bfile} FROM ${tabella_output}_new;" >> ${tabella_output}.hql \
                        && echo "DROP TABLE ${tabella_output}_new;" >> ${tabella_output}.hql \
                ) && ( \
                        hive -f ${tabella_output}.hql && rm -f ${tabella_output}.hql \
                ) &>> esito_hive_${tabella_output}.log \
                && ( \
                        ([ $(cat esito_hive_${tabella_output}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Creazione tabella ${tabella_output^^}_p completata con successo." >> esito_hive_${tabella_output}.log ) \
                        || echo "$(date) ERRORE nella creazione della tabella ${tabella_output^^}_p" &>> esito_hive_${tabella_output}.log \
                )
            cat esito_sqoop_${tabella}.log &>> "$F_LOG"
            cat esito_hive_${tabella_output}.log &>> "$F_LOG"

            rm -f esito_sqoop_${tabella}.log
            rm -f esito_hive_${tabella_output}.log

            rm -f "./"$tabella"_proc.logi"

            echo "$(date) Importazione tabella ${tabella^^} completato"  &>> "$F_LOG"
            echo "$(date) Importazione tabella ${tabella^^} completato"
}

read_rcu_and_import () {
        eval F_LOG="$1"

        echo "$(date) Avvio aggiornato tabelle da RCU " &> "$F_LOG" \

        for ((i =0; i < ${#tabelle_schemi[@]}; i++))
        do
                if [ $((i%4)) -eq 0 ]
                then

                 tabella=${tabelle_schemi[$i]}
                 tabella_output=${tabelle_schemi[$i+1]}
                 schema=${tabelle_schemi[$i+2]}
                 vista_tabella=${tabelle_schemi[$i+3]}
                 num_mappers="30"

                 if [[ $vista_tabella == "tabella" ]]
                 then
                    schema_nocorpo=$(echo ${schema} | sed --expression='s/T_CORPO[A-Z_]* string, //g')
                    #colonne_bfile=$(echo ${schema} | awk 'BEGIN {RS="string, "; ORS=""} /T_CORPO[A-Z_]*/ {print ",'\'\'' AS " $0}')
                    colonne_bfile=""
                 else
                    schema_nocorpo=$(echo ${schema})
                    colonne_bfile=""
                    num_mappers="1"
                 fi

                 echo "TABELLA=$tabella TABELLA_OUTPUT=$tabella_output SCHEMA=$schema_nocorpo COLONNE=$colonne_bfile MAPS=$num_mappers"
                 rm -f "./"$tabella"_proc.logi"
                 echo "in_process" > "./"$tabella"_proc.logi"

                 do_import "\${F_LOG}" "\${tabella}" "\${num_mappers}" "\${schema_nocorpo}" "\${colonne_bfile}" "\${tabella_output}" & 


                fi

        done

        return
        in_import=true

        while $in_import; do

        sleep 3

        num_import=$(ls -all | grep ".logi" | wc -l)


        if [ $((num_import)) -eq 0 ];then in_import=false; fi

        done



        echo ""
        echo "$(date) Tutte le operazioni sono concluse"
        echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"

}

