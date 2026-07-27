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
	rcugas.rcugas_massivo "N_ID_PDR string, T_CODICE_PDR string, CAPACITA_TRASPORTO string, MESE_VAL_CAP_TRASP string, T_COD_TIPO_PDR string, T_DISALIMENTABILITA string, BILANCIAMENTO string, N_ID_FORNITURA string, D_DATA_INIZIO_FOR string, DATA_FINE_FOR string, N_ID_AZ_UDD string, PIVA_UDD string, N_ID_AZ_CC string, PIVA_CC string, N_ID_CLIENTE string, T_PARTITA_IVA_CLI string, T_CODICE_FISCALE_CLI string, B_CF_STRANIERO string, T_REFERENTE string, T_NOME_REF string, T_COGNOME_REF string, T_EMAIL_REF string, T_TELEFONO_REF string, T_RESIDENZA string, DATA_VAL_RES string, T_TOPONIMOPDR string, T_NOMESTRADA_PDR string, T_CIVICO_PDR string, T_CAP_PDR string, T_COMUNE_ISTAT_PDR string, T_COMUNE_PDR string, T_PROVINCIA_PDR string, T_NAZIONE_PDR string, ALTRO_IND_PDR string, T_TOPONIMO_FORN string, T_NOMESTRADA_FORN string, T_CIVICO_FORN string, T_CAP_FORN string, T_COMUNE_ISTATFORN string, T_COMUNE_FORN string, T_PROVINCIA_FORN string, T_NAZIONE_FORN string, ALTRO_IND_FORN string, T_ACCESSO_UI string, T_TIPO_FORNITURA string, T_ALIQUOTA_IVA string, T_ALIQUOTA_ACCISE string, T_ADD_REGIONALE string, T_ALTRE_INFO_IMPOSTE string, T_MATRICOLA_MISURATORE string, T_CLASSE_MISURATORE string, T_TIPO_MISURATORE string, T_TELEGESTIONE string, T_PRE_CONV string, T_MATRICOLA_CONVERTITORE string, N_NUM_CIFRE_CONVERTITORE string, T_ANNO_FABBRIC_CONVERTITORE string, T_DATA_INST_CONVERTITORE string, N_COEFF_CORREZIONE string, PRESS_MISURE string, T_ACCESS_MISURATORE string, N_NUM_CIFRE_MISURATORE string, T_ANNO_FABBRIC_MISURATORE string, T_DATA_INST_MISURATORE string, T_MISURATORE_INTEGRATO string, N_POTENZIALITA_MASSIMA string, N_POTENZIALITA_TOT_INSTALLATA string, N_MAX_PRELIEVO_ORARIO string, T_EROG_SERVIZIO_ENERG string, T_PARTITA_IVA_GESTCAL string, T_RAGIONE_SOCIALE_GESTCAL string, T_TELEFONO_GESTCAL string, T_EMAIL_GESTCAL string, T_TOPONIMO_GESTCAL string, T_NOMESTRADA_GESTCAL string, T_CIVICO_GESTCAL string, T_CAP_GESTCAL string, T_COMUNE_ISTAT_GESTCAL string, T_COMUNE_GESTCAL string, T_PROVINCIA_GESTCAL string, T_NAZIONE_GESTCAL string, T_INDIRIZZO_COMPLETO string, D_DATA_RIF_PDR string, D_AGGIORNAMENTO_PDR string, D_DATA_RIF_TECN string, D_AGGIORNAMENTO_TECN string, D_DATA_RIF_MIS string, D_AGGIORNAMENTO_MIS string, D_DATA_RIF_FORN string, D_AGGIORNAMENTO_FORN string, T_TIPO_BONUS string, D_DATA_INIZIO_EROG_BONUS string, D_DATA_FINE_EROG_BONUS string, D_DATA_RIF_BONUS string, D_AGGIORNAMENTO_BONUS string, D_DATA_AGGIORNAMENTO string, N_ID_UDD string, N_ID_VENDITORE string, T_COD_PROFILO string, T_COD_CAT_USO string, T_COD_CLASSE_PRELIEVO string, T_ANNO_TERMICO string, D_DATA_RIF_PREL string, T_TRATTAMENTO string, T_TOPONIMO_ESAZ string, T_NOMESTRADA_ESAZ string, T_CIVICO_ESAZ string, T_CAP_ESAZ string, T_COMUNE_ISTAT_ESAZ string, T_COMUNE_ESAZ string, T_PROVINCIA_ESAZ string, T_NAZIONE_ESAZ string, ALTRO_IND_ESAZ string, T_CODICE_ATECO string, T_PAGAMENTO_IVA string, T_CODICE_UFFICIO string, T_CF_INTESTATARIO_FATT string, T_CF_STRANIERO_FATT string, T_PIVA_INTESTATARIO_FATT string, T_NOME_INTESTATARIO_FATT string, T_COGNOME_INTESTATARIO_FATT string, T_RAG_SOC_INTESTATARIO_FATT string, T_ANNO_MESE_RINN_BONUS string, D_DATA_INIZIO_BONUS string, D_DATA_FINE_BONUS string, N_PRELIEVO_ANNUO string, T_FATTORE_CORREZ_CLIMATICA string, T_ALTRO_IND_GESTCAL string, T_TIPO_OP string, T_PROCESSO string,n_id_pratica_processo STRING" \
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
	
		do_import "\${tabella}" "\${schema}" "\${schema_nocorpo}" "\${colonne_bfile}" &
	
	 
	fi
	
done

echo ""
echo "$(date) Tutte le operazioni sono concluse"
echo "$(date) Tutte le operazioni sono concluse" &>> "$F_LOG"

