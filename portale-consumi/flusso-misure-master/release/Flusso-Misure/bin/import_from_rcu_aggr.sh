#!/bin/bash


do_import () {

		eval tabella="$1"
		eval schema="$2"

		eval schema_nocorpo="$3"
		eval colonne_bfile="$4"
		

		
		# Metto un 'a capo' tra uno scarico e l'altro
		echo ""
		
		# Avvio gli scarichi
		echo "$(date) Scarico tabella ${tabella^^} in corso..."  \
		&& ( \
			hdfs dfs -rm -R -f -skipTrash /acquirente_unico/n${tabella}/ \
			; sqoop import --connect  jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${tabella} -m 60 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /acquirente_unico/n${tabella} --direct \
		) &> esito_sqoop_${tabella}.log \
		&& ( \
			([ $(cat esito_sqoop_${tabella}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Importazione dati tabella ${tabella^^} completata con successo." > esito_sqoop_${tabella}.log ) \
			|| echo "$(date) ERRORE nell'importazione dei dati" &>> esito_sqoop_${tabella}.log \
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
		cat esito_sqoop_${tabella}.log
                cat esito_hive_${tabella}.log 

	        rm -f esito_sqoop_${tabella}.log
 	        rm -f esito_hive_${tabella}.log
		
		rm -f "./"$tabella"_proc.logi"
		
		echo "$(date) Scarico tabella ${tabella^^} completato"
}

tabelle_schemi=( \
	rcus.rcus_podmisure "N_ID_SCHEDA string, N_ID_POD string, D_ANNO_MESE string, T_TRATTAMENTO string, T_TRATTAMENTO_SUCC string, N_CONSUMO_ANNUO string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string" \
	rcu.rcu_pod "N_ID_POD string, T_CODICE_POD string, T_AREA_RIF string, B_RICH_INDENNIZZO string, B_RICH_PREST_DISTR string, N_ID_INDIRIZZO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_IND_FORN string" \
	rcu.rcu_pod_stato "N_ID_POD string, T_STATO_ATTIVAZIONE string, D_ATTIVAZIONE string, D_DISATTIVAZIONE string, T_CAUSALE_NO_RIATTIV string, T_CAUSALE_NO_DISATTIV string, T_STATO_SOSP string, D_SOSPENSIONE string, D_REVOCA_SOSP string, T_CAUSALE_NO_SOSP string, T_SWITCHING string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_COD_DISATTIVAZIONE string" \
	rcus.rcus_podstato "N_ID_SCHEDA string,N_ID_POD string, T_STATO_ATTIVAZIONE string, D_ATTIVAZIONE string, D_DISATTIVAZIONE string, T_CAUSALE_NO_RIATTIV string, T_CAUSALE_NO_DISATTIV string, T_STATO_SOSP string, D_SOSPENSIONE string, D_REVOCA_SOSP string, T_CAUSALE_NO_SOSP string, T_SWITCHING string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string,N_ID_TRACCIA string, N_ID_S_PREC string,N_ID_S_SUCC string,B_VALIDO string, T_COD_DISATTIVAZIONE string" \
	rcu.rcu_pod_misure "N_ID_POD string, D_ANNO_MESE string, T_TRATTAMENTO string, T_TRATTAMENTO_SUCC string, N_CONSUMO_ANNUO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
	tmpod_cloud.forzare_trattamento_tot_am "POD STRING,D_ANNO_MESE STRING" \
)


for ((i = 0; i < ${#tabelle_schemi[@]}; i++))
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
	
		do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}" &
	
	 
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


