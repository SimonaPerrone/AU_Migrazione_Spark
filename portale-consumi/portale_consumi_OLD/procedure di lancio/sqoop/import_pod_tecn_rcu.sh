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
			hdfs dfs -rm -R -f -skipTrash /acquirente_unico/n${tabella}/ \
			; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m 60 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /acquirente_unico/n${tabella} --direct \
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
		
		
		echo "$(date) Importazione tabella ${tabella^^} completato"  &>> "$F_LOG"
		echo "$(date) Importazione tabella ${tabella^^} completato"
}

tabelle_schemi=( \
	rcu.rcu_pod_tecn "N_ID_POD string, N_POTENZA_DISPONIBILE string, N_POTENZA_IMPEGNATA string, N_TENSIONE string, T_TIPO_MISURATORE string, N_K_TRASFORMAZIONE string, D_INST_MISURATORE string, D_RIMOZ_MISURATORE string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_NUM_CIFRE_EA string, N_NUM_CIFRE_ER string, N_K_TRASFOR_ATT string, N_K_TRASFOR_REA string, N_K_TRASFOR_POT string, T_MAT_MISURATORE_ATT string, T_MAT_MISURATORE_REA string, T_MAT_MISURATORE_POT string, D_INST_MISURATOR_ATT string, D_INST_MISURATOR_REA string, D_INST_MISURATOR_POT string, N_NUM_CIFRE_ATT string, N_NUM_CIFRE_REA string, N_NUM_CIFRE_POT string, B_PRESENZA_MIS string, B_GEST_FORFAIT string, T_TIPO_POD string, D_FINE_TIPO_POD string, D_OPER_MISURATOR_ATT string, D_OPER_MISURATOR_REA string, D_OPER_MISURATOR_POT string, T_MOTIVAZIONE string" \
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

		
		do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}" 
	
	 
	fi
	
done


echo "$(date) Tutte le operazioni sono concluse"


