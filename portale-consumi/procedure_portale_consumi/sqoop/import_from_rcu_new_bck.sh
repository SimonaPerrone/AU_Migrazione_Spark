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
	rcu.rcu_pod_tecn "N_ID_POD string, N_POTENZA_DISPONIBILE string, N_POTENZA_IMPEGNATA string, N_TENSIONE string, T_TIPO_MISURATORE string, N_K_TRASFORMAZIONE string, D_INST_MISURATORE string, D_RIMOZ_MISURATORE string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_NUM_CIFRE_EA string, N_NUM_CIFRE_ER string, N_K_TRASFOR_ATT string, N_K_TRASFOR_REA string, N_K_TRASFOR_POT string, T_MAT_MISURATORE_ATT string, T_MAT_MISURATORE_REA string, T_MAT_MISURATORE_POT string, D_INST_MISURATOR_ATT string, D_INST_MISURATOR_REA string, D_INST_MISURATOR_POT string, N_NUM_CIFRE_ATT string, N_NUM_CIFRE_REA string, N_NUM_CIFRE_POT string, B_PRESENZA_MIS string, B_GEST_FORFAIT string, T_TIPO_POD string, D_FINE_TIPO_POD string, D_OPER_MISURATOR_ATT string, D_OPER_MISURATOR_REA string, D_OPER_MISURATOR_POT string, T_MOTIVAZIONE string" \
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
	prt_rcugas.rcugas_temp_va1 "N_ID_PRATICA string, PROTOCOLLO string, DATA_APERTURA string, N_ID_UTENTE string, N_ID_VENDITORE string, COD_PRESTAZIONE string, DATA_ESECUZIONE string, PIVA_UDD string, COD_PDR string, COD_REMI string, TIPO_PDR string, COD_PROF_PREL_STANDARD string, PRELIEVO_ANNUO_PREV string, CF string, PIVA string, CF_STRANIERO string, NOME string, COGNOME string, RAGIONE_SOCIALE string, DATA_INIZIO string, DATA_FINE string, CODICE_COMUNE string, N_ID_CAUSALE string, COD_ESITO string, AMMISSIBILE string, TIPO_FORNITURA string" \
	rcu.rcu_azienda "N_ID_AZIENDA string, N_ID_UTENTE string, T_CODICE_AEEG string, T_PIVA string, T_CF string, T_RAG_SOC string, N_ID_SEDELEGALE string, T_CONTATTO string, T_EMAIL string, T_PEC string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
	rcu.rcu_clientefinale "N_ID_CLIENTE string, T_NOME string, T_COGNOME string, T_RAGSOC string, B_PERSONA_FISICA string, T_CF string, T_PIVA string, N_ID_SEDELEGALE string, T_EMAIL string, T_CODICE_ATECO string, B_DIRITTO_MT string, D_AUTOCERT_MT string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_DENOM string, T_DETTAGLIO_CF string, T_DETTAGLIO_PIVA string" \
	rcu.rcu_fasce_misuratore_2g "N_ID_FASCE_MISURATORE_2G string, N_ID_MISURATORE string, N_COD_GIORNO_2G string, D_DATA_GIORNO string, N_FASCIA_1 string, N_FINE_FASCIA_1 string, N_FASCIA_2 string, N_FINE_FASCIA_2 string, N_FASCIA_3 string, N_FINE_FASCIA_3 string, N_FASCIA_4 string, N_FINE_FASCIA_4 string, N_FASCIA_5 string, N_FINE_FASCIA_5 string, N_FASCIA_6 string, N_FINE_FASCIA_6 string, N_FASCIA_7 string, N_FINE_FASCIA_7 string, N_FASCIA_8 string, N_FINE_FASCIA_8 string, N_FASCIA_9 string, N_FINE_FASCIA_9 string, N_FASCIA_10 string, N_FINE_FASCIA_10 string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string" \
	rcu.rcu_fornitura "N_ID_FORNITURA string, N_ID_POD string, N_ID_CLIENTE string, D_INIZIO_TITOLARITA string, D_FINE_TITOLARITA string, T_COD_CONTRATTO string, D_STIPULA_CONTRATTO string, D_MESE_RINNOVO string, N_IVA string, B_DISALIMENTABILITA string, T_TARIFFA_DISTR string, T_CODICE_ATECO string, N_ID_FORNITORE string, T_RUOLO_FORNITORE string, T_TIPO_MERCATO string, B_SALVAGUARDIA string, T_BONUS_SOCIALE string, D_INIZIO_BONUS string, D_FINE_BONUS string, T_COMUNIC_BONUS string, N_IMPOSTE string, N_ID_INDIR_ESAZIONE string, N_ID_INDIR_COMUNIC string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_DIRITTO_TUTELA string, T_CODICE_UFFICIO string, T_PAGAMENTO_IVA string, T_ADDIZ_PROVINCIALE string, T_ADDIZ_COMUNALE string, T_TELEFONO string, T_IVA string, T_IMPOSTE string" \
	rcu.rcu_indirizzo "N_ID string, T_TOPONIMO string, T_NOMESTRADA string, T_CIVICO string, T_COMUNE string, T_COMUNE_ISTAT string, T_CAP string, T_PROVINCIA string, T_NAZIONE string, T_INDIRIZZO_COMPLETO string, T_NOTA string" \
	rcu.rcu_misuratore_2g "N_ID_MISURATORE_2G string, N_ID_POD string, B_VIS_FASCE string, B_VIS_VENDITORE string, B_VIS_TELEFONOV string, B_VIS_DATAINICONTR string, B_VIS_DATAINIZIOFREEZING string, B_VIS_MESSAGGICLIENTE string, B_VIS_CODCLI string, T_CODCLI string, T_VENDITORE string, T_TELEFONOV string, D_DATA_INICONTR string, D_DATA_INIZIOFREEZING string, T_MESSAGGIO_CLIENTE_1 string, T_MESSAGGIO_CLIENTE_2 string, T_MESSAGGIO_CLIENTE_3 string, T_MESSAGGIO_CLIENTE_4 string, T_MESSAGGIO_CLIENTE_5 string, N_NUM_FASCE string, D_INIZIO_VALIDITA string, D_FINE_VALIDITA string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_DATA_RIF string, T_TIPO_CONFIGURAZIONE string" \
	rcu.rcu_pod "N_ID_POD string, T_CODICE_POD string, T_AREA_RIF string, B_RICH_INDENNIZZO string, B_RICH_PREST_DISTR string, N_ID_INDIRIZZO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_IND_FORN string" \
	rcu.rcu_pod_distr "N_ID_POD string, N_ID_DISTR string, D_INIZIO string, D_FINE string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
	rcu.rcu_pod_misure "N_ID_POD string, D_ANNO_MESE string, T_TRATTAMENTO string, T_TRATTAMENTO_SUCC string, N_CONSUMO_ANNUO string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
	rcu.rcu_pod_stato "N_ID_POD string, T_STATO_ATTIVAZIONE string, D_ATTIVAZIONE string, D_DISATTIVAZIONE string, T_CAUSALE_NO_RIATTIV string, T_CAUSALE_NO_DISATTIV string, T_STATO_SOSP string, D_SOSPENSIONE string, D_REVOCA_SOSP string, T_CAUSALE_NO_SOSP string, T_SWITCHING string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, T_COD_DISATTIVAZIONE string" \
	rcu.rcu_pod_udd "N_ID_POD string, N_ID_UDD string, D_INIZIO string, D_FINE string, D_STIPULA string, T_NOTA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string" \
	rcu.rcu_residenza "N_ID string, N_ID_FORNITURA string, T_RESIDENTE string, D_INIZIO_RESIDENZA string, D_FINE_RESIDENZA string, B_STORICO string, B_VALIDO string, B_ULTIMA string, N_ID_TRACCIA string, D_AGGIORNAMENTO string" \
	rcu.rcu_tariffa "N_ID_TARIFFA string, N_ID_FORNITURA string, T_TARIFFA_DISTR string, D_INIZIO_TARIFFA string, D_FINE_TARIFFA string, B_STORICO string, B_VALIDO string, B_ULTIMA string, N_ID_TRACCIA string, D_AGGIORNAMENTO string" \
	rcu.rcu_udd "N_ID_UDD string, T_CODICE_TERNA string, D_AGGIORNAMENTO string, N_ID_TRACCIA string, N_ID_S_PREC string, D_INIZIO string, D_FINE string, N_ID_AZIENDA_RIF string, T_TIPO string" \
	rcu.rcu_distr_emt "N_ID_GESTMT STRING ,N_ID_DISTR STRING,N_ID_EMT STRING,T_AREA_MT STRING,D_INIZIO STRING,D_FINE STRING,D_AGGIORNAMENTO STRING,N_ID_TRACCIA STRING,N_ID_S_PREC STRING,T_COD_DISTR STRING" \
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
	rcus.rcus_pod "N_ID_SCHEDA string, N_ID_POD string, T_CODICE_POD string, T_AREA_RIF string, B_RICH_INDENNIZZO string, B_RICH_PREST_DISTR string, N_ID_INDIRIZZO string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string, N_ID_IND_FORN string" \
	rcus.rcus_pod_distr "N_ID_SCHEDA string, N_ID_POD string, N_ID_DISTR string, D_INIZIO string, D_FINE string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string" \
	rcus.rcus_pod_podtecn "N_ID_S_POD string, N_ID_S_PODTECN string, D_AGGIORNAMENTO string" \
	rcus.rcus_pod_udd "N_ID_SCHEDA string, N_ID_POD string, N_ID_UDD string, D_INIZIO string, D_FINE string, D_STIPULA string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string" \
	rcus.rcus_podmisure "N_ID_SCHEDA string, N_ID_POD string, D_ANNO_MESE string, T_TRATTAMENTO string, T_TRATTAMENTO_SUCC string, N_CONSUMO_ANNUO string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string" \
	rcus.rcus_podtecn "N_ID_SCHEDA string, N_ID_POD string, N_POTENZA_DISPONIBILE string, N_POTENZA_IMPEGNATA string, N_TENSIONE string, T_TIPO_MISURATORE string, N_K_TRASFORMAZIONE string, D_INST_MISURATORE string, D_RIMOZ_MISURATORE string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string, N_ID_TRACCIA string, N_ID_S_PREC string, N_ID_S_SUCC string, B_VALIDO string, N_NUM_CIFRE_EA string, N_NUM_CIFRE_ER string, N_K_TRASFOR_ATT string, N_K_TRASFOR_REA string, N_K_TRASFOR_POT string, T_MAT_MISURATORE_ATT string, T_MAT_MISURATORE_REA string, T_MAT_MISURATORE_POT string, D_INST_MISURATOR_ATT string, D_INST_MISURATOR_REA string, D_INST_MISURATOR_POT string, N_NUM_CIFRE_ATT string, N_NUM_CIFRE_REA string, N_NUM_CIFRE_POT string, B_PRESENZA_MIS string, B_GEST_FORFAIT string, T_TIPO_POD string, D_FINE_TIPO_POD string, D_OPER_MISURATOR_ATT string, D_OPER_MISURATOR_REA string, D_OPER_MISURATOR_POT string, T_MOTIVAZIONE string" \
	rcus.rcus_podstato "N_ID_SCHEDA string,N_ID_POD string, T_STATO_ATTIVAZIONE string, D_ATTIVAZIONE string, D_DISATTIVAZIONE string, T_CAUSALE_NO_RIATTIV string, T_CAUSALE_NO_DISATTIV string, T_STATO_SOSP string, D_SOSPENSIONE string, D_REVOCA_SOSP string, T_CAUSALE_NO_SOSP string, T_SWITCHING string, T_NOTA string, D_AGGIORNAMENTO string, D_ARCHIVIAZIONE string,N_ID_TRACCIA string, N_ID_S_PREC string,N_ID_S_SUCC string,B_VALIDO string, T_COD_DISATTIVAZIONE string" \
	switch_gas.prt_swg "N_ID_SWG string, N_ID_PRATICA string, T_CODICE_PDR string, T_STATO string, N_ID_RICH string, N_ID_DISTR string, N_ID_CC_ENTRANTE string, N_ID_CC_USCENTE string, N_ID_UDD_USCENTE string, T_TITOLO string, D_DATA_RICHIESTA string, T_CP_UTENTE string, T_CP_DISTRIBUTORE string, T_CF_CLIENTE_FINALE string, T_PIVA_CLIENTE_FINALE string, B_CF_STRANIERO string, B_PERSONA_FISICA string, D_DATA_CONTRATTO string, D_DATA_DECORRENZA string, B_AMMISSIBILE string, T_COD_CAUSALE string, T_MOTIVAZIONE string, T_ESITO string, T_COD_ESITO string, T_DETT_ESITO string, D_DATA_STATO string, D_DATA_INSERIMENTO string, T_TIPO_FORNITURA string, T_PIVA_CC string, T_REVOCA string, T_TIMG_PDR_CHIUSO_MOR string, T_TIMG_DATE_SOSP string, T_TIMG_DATE_SOST string, T_TIMG_ACC_MIS string, T_TIMG_PRESENZA_CMOR string, D_TIVG_DATA_ATT_FDD string, D_TIVG_DATA_CHIUSURA_PDR string, B_TIMG_TIVG_CALCOLATO string, B_ANN_IN_RITARDO string" \
	switch_gas.prt_vtg "N_ID_VTG string, N_ID_PRATICA string, T_STATO string, T_CODICE_PDR string, T_CODICE_REMI string, D_DATA_RICHIESTA string, D_DATA_DEC_RICHIESTA string, D_DATA_DEC string, T_TITOLO string, N_ID_NVG1 string, B_AMMISSIBILE string, T_COD_CAUSALE string, T_MOTIVO string, N_ID_UTENTE string, T_PIVA_UTENTE string, T_RUOLO_UTENTE string, T_CP_UTENTE string, N_ID_DISTR string, T_CP_DISTR string, N_ID_UDD_U string, N_ID_CC_U string, N_ID_UDD_RCU_E string, N_ID_CC_E string, T_RUOLO_CC_E string, T_PIVA_CC string, N_ID_CLIENTE_FINALE string, N_ID_FORNITURA string, N_ID_DISPACCIAMENTO string, N_ID_COD_CAUSALE_DISTR string, T_TIPOLOGIA_VOLTURA string, T_TIPO_FORNITURA string, T_CF_COD_FISC string, T_CF_PIVA string, B_CF_COD_FISC_ESTERO string, B_CF_PERSONA_FISICA string, T_ESITO string, T_COD_ESITO string, T_DETT_ESITO string, N_MAX_ORE_CONG string, D_DATA_STATO string" \
	switch_gas.prt_vtg_operazione  "N_ID string, N_ID_VTG string, T_COD_OP string, N_ID_UTENTE string, N_ID_OPERATORE string, D_RIC_RICH string, D_ESECUZIONE string" \
	swtch.prt_se "N_ID_SE string, N_ID_PRATICA string, T_STATO string, T_PROT_RICH string, T_CODICE_POD string, T_CLI_RCU_CF string, T_CLI_RCU_PIVA string, B_CLI_CF_STRANIERO string, B_CLI_PIVA_ESTERA string, D_DATA_CONTRATTO string, D_DATA_DECORRENZA string, B_REVOCA_TIMOE string, B_ACQUISTO_CREDITO string, T_COD_CONTR_DISP string, T_CONTR_CONNESSIONE string, B_AMMISSIBILE string, T_COD_CAUSALE string, T_MOTIVAZIONE string, N_ID_RICH string, T_RUOLO_RICH string, N_ID_DISTR string, N_ID_UDD_U string, N_ID_CC_U string, N_ID_CC_E string, T_PROT_DISTR string, B_DATI_TIMOE string, T_COD_ESITO string, T_DETT_ESITO string, D_DATA_FLUSSO string, D_DATA_STATO string, B_INVALIDATA string, N_ID_CLIENTE_RCU string, N_ID_RIFERIMENTO_EVENTO string, N_ID_TIPO_EVENTO string, B_INFRAMESE string" \
	tisg.prt_sag "N_ID_SAG string, N_ID_PRATICA string, T_TIPO_PRT string, T_ANNO string, N_ID_UTENTE string, T_STATO string, D_RICHIESTA string, B_AMMISSIBILE string, B_FUORI_FINESTRA string, T_COD_CAUSALE string, T_MOTIVAZIONE string, D_DATA_STATO string, T_PIVA_UTENTE string" \
	tisg.prt_sag_file "N_ID_FILE string, N_ID_SAG string, T_NOME string, T_TIPO string, T_TIPO_TRACCIATO string, N_ID_PADRE string, T_CORPO string, B_AMMISSIBILE string, T_STATO_FILE string, N_DIMENSIONE string, T_DIGEST string, N_ID_OPERAZIONE string, T_IDENT_FILE string, D_UPLOAD string, D_TRASMISSIONE_UTENTE string, N_NUM_FILE_CONTENUTI string, N_RIGHE string, T_MOTIVAZIONE string, D_DOWNLOAD string, T_COD_INAMMISSIBILITA string, T_ANNO_RIF string, T_MESE_RIF string, T_PIVA_UTENTE string, D_ELABORAZIONE string, B_FILE_LOADED string, B_FILE_ELABORATO string" \
	tmpod.prt_tmo "N_ID_TMO string, N_ID_PRATICA string, T_TIPO_PRT string, T_ANNO string, T_MESE string, N_ID_UTENTE string, T_STATO string, D_RICHIESTA string, B_AMMISSIBILE string, B_FUORI_FINESTRA string, T_COD_CAUSALE string, T_MOTIVAZIONE string, D_DATA_STATO string, FLAG_GESTORE string" \
	tmpod.prt_tmo_aggregati_calcolati "N_ID string, N_ID_DISTR string, T_AREA_RIF string, ANNOMESE string, N_ID_UDD string, GIORNO string, N_H1 string, N_H2 string, N_H3 string, N_H4 string, N_H5 string, N_H6 string, N_H7 string, N_H8 string, N_H9 string, N_H10 string, N_H11 string, N_H12 string, N_H13 string, N_H14 string, N_H15 string, N_H16 string, N_H17 string, N_H18 string, N_H19 string, N_H20 string, N_H21 string, N_H22 string, N_H23 string, N_H24 string, N_H25 string, D_DATA_AGGREGAZIONE string, T_AGGR_SOTTESI string, N_ID_DISTR_RIF string, UID_ELAB string" \
	tmpod.prt_tmo_file "N_ID_FILE string, N_ID_TMO string, T_NOME string, T_TIPO string, T_TIPO_TRACCIATO string, N_ID_PADRE string, T_CORPO string, B_AMMISSIBILE string, T_STATO_FILE string, N_DIMENSIONE string, T_DIGEST string, N_ID_OPERAZIONE string, T_IDENT_FILE string, D_UPLOAD string, D_TRASMISSIONE_UDD string, N_ANNO_RIF string, N_MESE_RIF string, N_NUM_FILE_CONTENUTI string, N_NUM_POD string, T_CODICE_DP string, N_ID_CODICE_INAMMISSIBILITA string, T_MOTIVAZIONE string, PRESENTE_DB string, B_COPIATO string" \
	userappl.t001_app_prt_pratiche "N_ID_PRATICA string, T_PROTOCOLLO string, N_ID_DESCRITTORE_PROCESSO string, T_STATO string, N_ID_UTENTE string, N_ID_OPERATORE string, D_DATA_APERTURA string, D_DATA_CHIUSURA string, T_POD string, N_CONTATORE_MODIFICA string, T_STATO_BUSINESS string, T_ARCHIVIATA string, N_ID_UTENTE_MODIFICA string, N_ID_PRATICA_ORIGINE string, N_ID_OPERATORE_CHIUSURA string, T_VISIBILE string, T_URL_ANNULL string" \
	userappl.t033_app_cpf_ruoli_au "N_ID_RUOLO_AU string, T_COD_RUOLO_AU string, T_DES_RUOLO_AU string, T_UTENTE_INSERIMENTO string, D_DATA_INSERIMENTO string, T_UTENTE_MODIFICA string, D_DATA_MODIFICA string, T_CODICE_AU string, T_TIPO_ATTIVITA string, T_FLG_OBBLIGATORIETA string, T_COMMODITY string" \
	userappl.t035_app_cpf_utenti_ruoli_au "N_ID_UTENTE string, N_ID_RUOLO_AU string, T_COD_AUTORITA string" \
	tmpod_cloud.forzare_trattamento_tot_am "POD STRING,D_ANNO_MESE STRING" \
)
#	tmpod.prt_tmo_mn "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, PUNTODISPACCIAMENTO string, DATAMISURA string, MOTIVAZIONE string, TRATTAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, D_RICEZIONE string, PERDITA string, ID_PROC string, GIORNOMISURA string, GRUPPOMIS string, FORFAIT string, KR string, KP string, MATRATT string, MATRREA string, MATRPOT string, DATAINSTMISATT string, DATAINSTMISREA string, DATAINSTMISPOT string, CIFREPOT string" \
 #       tmpod.prt_tmo_mv "N_ID string, N_ID_FILE string, N_ID_UDD string, N_ID_DISTR string, PIVA_DISTR string, TIPO_PRATICA string, COD_FLUSSO string, CODICE_POD string, ANNOMESE string, DATAVOLTURA string, DATARILEVAZIONE string, TRATTAMENTO string, PUNTODISPACCIAMENTO string, TENSIONE string, POTCONTRIMP string, POTDISP string, CIFREATT string, CIFREREA string, KA string, TIPOMISURA string, DATAINIZIOPERIODO string, RACCOLTA string, TIPODATO string, VALIDATO string, EAM string, EAF1 string, EAF2 string, EAF3 string, CODPRATATT string, KR string, ERM string, ERF1 string, ERF2 string, ERF3 string" \
#	switch_gas.prt_vtg6 "N_ID_VTG6 string, N_ID_VTG string, N_ID_PRATICA string, N_ID_UTENTE string, T_CODICE_PDR string, T_MATR_MIS string, D_DATA_ATT_CONTR string, N_VOL_ANNUO_SOST string, T_CLASSE_GRUPPO_MIS string, T_CIFRE_MIS string, T_SEGN_MIS_SOST string, T_PRE_CONV string, T_GRUPPO_MIS_INT string, N_COEFF_CORR string, T_MATR_CONV string, T_CIFRE_CONV string, T_SEGN_CONV string, D_DATA_MIS_EFF string, T_SEGN_MIS_EFF string, T_SEGN_CONV_EFF string, T_NOTE string, T_TIPO_LETTURA string, B_COPIATO_TMG_MISURE string" \



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
	RCUGAS.RCUGAS_IT_REMI "PIVA_IT string, CODICE_PDR string, REMI_POOL string, N_ID_REMI string" \
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
                        hdfs dfs -rm -R -f -skipTrash /acquirente_unico/n${vista}/ \
                        ; sqoop import --connect jdbc:oracle:thin:@scancl01-01.siiau.local:1521/SIIP --username pag_rcugas --password Psiiprdpag_rcugas5142f --table ${vista^^} -m 1 --null-string "\\\\N" --null-non-string "\\\\N" --fields-terminated-by '\b' --hive-delims-replacement ' ' --target-dir /acquirente_unico/n${vista} --direct \
                ) &> esito_sqoop_${vista}.log \
                && ( \
                        ([ $(cat esito_sqoop_${vista}.log | grep "100%" | wc -l) -eq 1 ] && echo "$(date) Importazione dati vista ${vista^^} completata con successo." > esito_sqoop_${vista}.log ) \
                        || echo "$(date) ERRORE nell'importazione dei dati vista ${vista} : " &>> esito_sqoop_${vista}.log \
                ) \
                && ( \
                        rm -f ${vista}.hql \
                        && echo "DROP TABLE IF EXISTS ${vista}_new;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_new ( ${schema} ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\b' STORED AS TEXTFILE LOCATION '/acquirente_unico/n${vista}';" >> ${vista}.hql \
                        && echo "DROP TABLE ${vista}_p;" >> ${vista}.hql \
                        && echo "CREATE TABLE ${vista}_p STORED AS PARQUET LOCATION '/acquirente_unico/${vista}_p' AS SELECT * FROM ${vista}_new;" >> ${vista}.hql \
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

