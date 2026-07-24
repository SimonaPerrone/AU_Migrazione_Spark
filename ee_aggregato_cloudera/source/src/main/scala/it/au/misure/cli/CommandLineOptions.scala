package it.au.misure.cli

import it.au.misure.commons.cli.{OptionGroup, Options, Option => CliOption}


/**
	* Commons-cli command line options for the Flusso-Misure tool suite.
	*/
@SerialVersionUID(114L)
class CommandLineOptions extends Serializable{
	// base options
	def version: CliOption = new CliOption("v", "version", false, "Stampa la versione e esci")
	def help: CliOption = new CliOption("h", "help", false, "Stampa l'help ed esci")
	def verbose: CliOption = new CliOption("V", "verbose", false, "Abilita log dettagliati (livello di log a debug)")
	
	//AU
	def decomprime2G: CliOption = new CliOption("D", "decomprimi", false, "Decomprime gli archivi 1G/2G su una cartella temporanea")

	def decomprimeAmmissibilita: CliOption = new CliOption("Da", "decomprimi_amm", false, "Decomprime gli archivi 1G/2G su una tabella temporanea per la fase di ammissibilità")
	def noDec: CliOption = new CliOption("n", "noDec", false, "Legge le directory non compresse")
	
	def injection: CliOption = new CliOption("i", "ingestione", false, "Ingestione dei flussi misura")
	def injection_new: CliOption = new CliOption("i_new", "ingestione_new", false, "Ingestione dei flussi misura con controlli di ammissibilità")

	def goAmmissibilita: CliOption = new CliOption("ia", "ingestione_amm", false, "Procedura di verifica ammissibilità dei file di misura da ingerire")

	def injection1G: CliOption = new CliOption("g", "1g", false, "Specifica l'ingestione dei flussi 1G")
	def injection2G: CliOption = new CliOption("G", "2g", false, "Specifica l'ingestione dei flussi 2G")
	def pdo: CliOption = new CliOption("t", "pdo", false, "Acquisisce misure PDO")
	def rfo: CliOption = new CliOption("T", "rfo", false, "Acquisisce misure RFO")
	def smis: CliOption = new CliOption("SS", "smis", false, "Acquisisce flussi SMIS")
	def other_f: CliOption = new CliOption("O", "altri_flussi", false, "Acquisisce flussi non PDO-RFO")

	def aggregatiOrari: CliOption = new CliOption("a", "aggregah", false, "Aggrega in misure orarie")
	def aggregatiOrari2: CliOption = new CliOption("a2", "aggregah2", false, "Aggrega in misure orarie new")
	def aggregatiOrari3: CliOption = new CliOption("a3", "aggregah3", false, "Aggrega in misure orarie new2")
	def aggregatiDettaglio: CliOption = new CliOption("a_dett", "aggrega_dett_h", false, "Dettaglio pod aggregato orario , indicare la versione orarie da utilizzare")

	def aggregatiAM: CliOption = new CliOption("b", "aggregaam", false, "Aggrega per AM")
	def aggiornamento: CliOption = new CliOption("u", "aggiorna", false, "Aggiorna le misure")
	def commitIN_fromtest_toprod : CliOption = new CliOption("Cin", "commit_ingestione", false, "Esegue il commit dei dati di gestione dal db di Collaudo al db di Produzione")
	def commitAG_fromtest_toprod : CliOption = new CliOption("Cag", "commit_aggregato", false, "Esegue il commit dei dati aggregati dal db di Collaudo al db di Produzione")
	def commitAL_fromtest_toprod : CliOption = new CliOption("Cal", "commit_tutto", false, "Esegue il commit di tutti i dati dal db di Collaudo al db di Produzione")
	def test: CliOption = new CliOption("k", "test", false, "Funzione di test")
	def rigenera_flusso_misure_quarti: CliOption = new CliOption("rg_fq", "rg_fq", false, "Rigenera tabella flusso misure quarti")
	def rigenera_flusso_misure_noaggr: CliOption = new CliOption("rg_fnag", "rg_fnag", false, "Rigenera tabella flusso misure noaggr")

	//def selInj2: CliOption = new CliOption("i2", "ingestione_alternativa", false, "Funzione per testare la seconda delle tre modalità di injection")
	//def selInj3: CliOption = new CliOption("i3", "ingestione_alternativa_3", false, "Funzione per testare la terza delle tre modalità di injection")

	def sem1: CliOption = new CliOption("S1", "sem_1", false, "Esegue la procedura di rettifica aggregato relativo ai 6 mesi precedenti l'anno corrente")
	def sem2: CliOption = new CliOption("S2", "sem_2", false, "Esegue la procedura di rettifica aggregato relativo ai 5 anni precedenti l'anno corrente")

	def sem_session: CliOption = new CliOption("SMS", "sem_session", true, "Indica il tipo di sessione della SEM(S1 o S2)")
	sem_session.setOptionalArg(false)

	def sem_commit: CliOption = new CliOption("SMC", "sem_commit", false, "Indica di eseguire la commit della SEM appena effettuata")

	def sem_invalidazioni: CliOption = new CliOption("SMI", "sem_invalida", false, "Cerca le misure da invalidare per motivazione 3 per tutti i mesi della sem di riferimento")

	def sem_force_tot: CliOption= new CliOption("Tot", "total", false, "Esegue la procedura di rettifica aggregato forzando il ricalcolo completo per tutti i mesi")
  def sem_force_anno_start:CliOption= new CliOption("SM_AL", "sem_annolancio", true, "Forza il lancio della Sem come anno iniziale a quello indicato nel parametro")
	sem_force_anno_start.setOptionalArg(false)

	def sem_importOraclePod_O: CliOption = new CliOption("ifo_pod_o", "import_from_oracle_pod_ora", false, "Esegue la procedura di importazione della vista POD_ORARI_2018 da oracle verso cloudera ")
	def sem_importOraclePod_F: CliOption = new CliOption("ifo_pod_f", "import_from_oracle_pod_fls", false, "Esegue la procedura di importazione delle misure(gennaio 2017 luglio 2018) da oracle verso cloudera ")
	def sem_importOracleDati: CliOption = new CliOption("ifo_dt", "import_from_oracle_dati", false, "Esegue la procedura di importazione dati da oracle verso cloudera necessari per la sem")
	def sem_importOracleAll: CliOption = new CliOption("ifo_all", "import_from_oracle", false, "Esegue la procedura di importazione di tutti i dati da oracle verso cloudera necessari per la sem")

	def portale_consumi: CliOption = new CliOption("PC_MS", "port_cons_ms", true, "Procedura di compilazione misure orarie e non orarie per il portale dei consumi (parametri O=orarie,N=non orarie,A=autoletture")
	def portale_consumi_export: CliOption = new CliOption("PC_EX", "port_cons_exp", true, "Procedura di esportazione misure orarie ,non orarie,autoletture e forniture per il portale dei consumi (parametri F=Forniture,N=non orarie,O=Orarie,A=autoletture")


	def decomprimeGAS: CliOption = {
		val o = new CliOption("DGAS", "decomprimi_gas", true, "Decomprime gli archivi del gas su una cartella temporanea")
		o.setArgName("TIPO_FLUSSO")
		o.setOptionalArg(true)
		o
	}


	def distrAgg: CliOption = {
	  val o = new CliOption("p", "disag", true, "pivadistributore in aggregati orari o n_id_distr in AM su cui generare l'aggregato")
	  o.setArgName("DISTR_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def uteAgg: CliOption = {
	  val o = new CliOption("P", "uteag", true, "pivautente in aggregati orari o n_id_udd in AM su cui generare l'aggregato")
	  o.setArgName("UTE_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def noDistrAgg: CliOption = {
	  val o = new CliOption("q", "nodisag", true, "pivadistributore in aggregati orari o n_id_distr in AM cui non generare l'aggregato")
	  o.setArgName("DISTR_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def noUteAgg: CliOption = {
	  val o = new CliOption("Q", "nouteag", true, "pivautente in aggregati orari o n_id_udd in AM cui non generare l'aggregato")
	  o.setArgName("UTE_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def distrUteAgg: CliOption = {
	  val o = new CliOption("d", "disute", true, "Acquisisce la lista di distributori utente specificati")
	  o.setArgName("DISUTE_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def annomesegiornodir: CliOption = {
		val o = new CliOption("z","ghigliottina", true, "Termine espresso come AAAAMMGG da cui considerare le aggregazioni. Default anno corrente <AAAA>, mese precedente <MM>, giorno 16 del mese")
		o.setArgName("AAAAMMGG")
		o.setOptionalArg(false)
		o
	}

	def writeCsvAggrOrario: CliOption = {
		val o = new CliOption("dett_csv","dett_csv", true, "Parametro per indicare la generazione o meno del file csv scritto su file system contenente il dettaglio dei pod a seguito dell'aggregato orario periodico o da sem  , valori possibili Y/N/timestamp da utilizzarescritto su file system(default N) ")
		o.setArgName("Y,N,AAAAMMGGHHMMSS")
		o.setOptionalArg(false)
		o
	}
	
	def anno: CliOption = {
	  val o = new CliOption("y", "anno", true, "Anno di riferimento")
	  o.setArgName("AAAA")
		o.setOptionalArg(false)
		o
	}
	def mese: CliOption = {
	  val o = new CliOption("m", "mese", true, "Mese di riferimento")
	  o.setArgName("MM")
		o.setOptionalArg(false)
		o
	}
	def giorno: CliOption = {
	  val o = new CliOption("s", "giorno", true, "Giorno di riferimento")
	  o.setArgName("GG")
		o.setOptionalArg(false)
		o
	}
	
		def giorni: CliOption = {
		val o = new CliOption("S", "giorni", true, "Sequenza di giorni di riferimento: \n" +
			"<G,G,G,G> solo i giorni specificati, \n " +
			"<,G,G,G>  dal primo del mese al primo della lista seguito dai giorni specificati \n" +
			"<G,G,G,>  i giorni specificati fino a fine mese \n" +
			"<G>       singolo giorno \n" +
			"<,G>      dal primo del mese al giorno specificato \n" +
			"<G,>      dal giorno specificato fino alla fine del mese \n" +
			"<,>       dal primo giorno del mese all'ultimo "
		)
		o.setValueSeparator(',')
		o.setArgName("G,G")
		o.setOptionalArg(false)
		o
	}

	def numMesi_PortaleConsumi: CliOption = {
		val o = new CliOption("nm", "num_mesi", true, "Numero di mesi da elaborare")
		o.setArgName("MM")
		o.setOptionalArg(false)
		o
	}

	def annomese_sem: CliOption = {
		val o = new CliOption("AM_S", "annomese", true, "Sequenza di anno mese (YYYYMM) per la sem: \n" +
			"<AAAAMM,AAAAMM,AAAAMM,AAAAMM> solo gli annimesi specificati, \n " +
			"<AAAAMM>       singolo annomese "
		)
		o.setValueSeparator(',')
		o.setArgName("AAAAMM,AAAAMM")
		o.setOptionalArg(false)
		o
	}

	def uid_aggr_triple: CliOption = {
		val o = new CliOption("v_am","versione_am", true, "versione orarie da considerare nell'aggregazione per triple per eseguire il salvataggio su Oracle")
		o.setArgName("AAAAMMGGHHMMSS")
		o.setOptionalArg(false)
		o
	}

	def local: CliOption = new CliOption("l", "locale", false, "Il processo viene eseguito localmente ( local[*] )")
	
	def aggrCommands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(distrUteAgg)
		og.addOption(distrAgg)
		og.addOption(uteAgg)
		og.addOption(noDistrAgg)
		og.addOption(noUteAgg)
		og
	}


	def semCommands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(sem_importOraclePod_O)
		og.addOption(sem_importOraclePod_F)
		og.addOption(sem_importOracleDati)
		og.addOption(sem_importOracleAll)
		og.addOption(sem_session)
		og.addOption(sem_invalidazioni)
		og
	}
	def semCommands2: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(sem_commit)
		og.addOption(sem_force_tot)
		og.addOption(sem_force_anno_start)
		og
	}

	def portaleCommands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(portale_consumi)
		og.addOption(portale_consumi_export)
		og
	}
	
	// option group for commands, as they are mutually exclusive
	def commands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(decomprime2G)
		og.addOption(decomprimeAmmissibilita)
		og.addOption(decomprimeGAS)
		og.addOption(injection)
		og.addOption(injection_new)
		og.addOption(goAmmissibilita)
		og.addOption(aggregatiOrari)
		og.addOption(aggregatiOrari2)
		og.addOption(aggregatiOrari3)
		og.addOption(aggregatiDettaglio)
		og.addOption(aggregatiAM)
//		og.addOption(aggregatiIP)
//		og.addOption(exportOracle)
		og.addOption(aggiornamento)
		og.addOption(test)
		og.addOption(rigenera_flusso_misure_quarti)
		og.addOption(rigenera_flusso_misure_noaggr)
		//og.addOption(selInj2)
		//og.addOption(selInj3)
		og.addOption(commitIN_fromtest_toprod)
		og.addOption(commitAG_fromtest_toprod)
		og.addOption(commitAL_fromtest_toprod)
		og.addOption(sem1)
		og.addOption(sem2)
	}
	
  def commandsPdoRfo: OptionGroup = {
		val os = new OptionGroup()
		os.addOption(pdo)
		os.addOption(rfo)
		os.addOption(smis)
		os.addOption(other_f)
	}
	
	def commandsGiorni: OptionGroup = {
		val os = new OptionGroup()
		os.addOption(giorno)
		os.addOption(giorni)
	}
	def commandAnnoMesi_SEM:OptionGroup={
		val os= new OptionGroup()
		os.addOption(annomese_sem)
	}
	
	// final options
	def getOptions: Options = {
		val os = new Options()
		os.addOption(version)
		os.addOption(help)
		os.addOption(verbose)
		os.addOption(anno)
		os.addOption(mese)
		os.addOption(numMesi_PortaleConsumi)
		os.addOption(local)
		os.addOption(annomesegiornodir)
		os.addOption(writeCsvAggrOrario)
		os.addOption(uid_aggr_triple)
		os.addOption(injection1G)
		os.addOption(injection2G)
		os.addOptionGroup(commands)
		os.addOptionGroup(commandsGiorni)
		os.addOptionGroup(commandsPdoRfo)
		os.addOptionGroup(aggrCommands)
		os.addOptionGroup(semCommands)
		os.addOptionGroup(semCommands2)
		os.addOptionGroup(commandAnnoMesi_SEM)
		os.addOptionGroup(portaleCommands)
		os
	}
	
}

