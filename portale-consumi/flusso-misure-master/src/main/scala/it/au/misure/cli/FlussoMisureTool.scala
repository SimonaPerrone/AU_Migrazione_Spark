package it.au.misure.cli

import org.apache.log4j.{Level, LogManager}
import it.au.misure.ingestione.Injection
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import java.util.concurrent.CountDownLatch
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import scala.util.{Failure, Success, Try}
import it.au.misure.ingestione.Decomprime12G
import it.au.misure.util.LoggingSupport

/**
 * ==FlussoMisureTool==
 * Rappresenta il punto di ingresso per utilizzare il processo Flusso Misure da linea di comando. 
 * Implementa la libreria Common CLI di Apache per leggere le opzioni della riga di comando passate al processo. E' anche in grado di stampare
 * messaggi di aiuto che dettagliano le opzioni disponibili. 
 */
object FlussoMisureTool extends LoggingSupport {
  
  val commonsCliUtils = new CommonsCliUtils()
	val commandLineOptions = new CommandLineOptions()
  
	def main(args: Array[String]) {
      
		/*
		 *  parse command line
		 */
	 
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)

		/*
		 * azioni di utilita'
		 */
		utility(commandLine)
		
		/*
		 * run the chosen command
		 */
		try{
		  runChosenCommand(commandLine,args)
		}catch{
		  case e: Exception => {
		    log.error(e.getMessage, e)
		    e.printStackTrace()
		  }
		  System.exit(1)
		}
		
	}

  /**
   * Stampa un messaggio di errore e chiude il programma.
   * @param message messaggio d'errore.
   */
	def printErrorAndExit(message: String): Unit = {
			log.info(message)
			log.info("Digita --help per informazioni")
			System.exit(0)
	}

	 /**
   * Stampa a video la versione del processo e chiude il programma.
   */
	def printVersionAndExit(): Unit = {
			log.info(s"Flusso Misure Tool - Versione 2.0.1")
			System.exit(0)
	}

	 /**
   * Stampa a video le opzioni disponibili del processo e chiude il programma.
   */
	def printHelpAndExit(): Unit = {
			commonsCliUtils.printHelpForOptions(commandLineOptions.getOptions)
			System.exit(0)
	}

  /**
   * Punto di ingresso per le varie funzionalità del processo in base all'argomento passato da riga di comando.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
   */
	def runChosenCommand(commandLine: CommandLine, args: Array[String]): Unit = {
	    val inizio = System.currentTimeMillis() / 1000

			if (commandLine.hasOption(commandLineOptions.decomprime2G.getOpt) || commandLine.hasOption(commandLineOptions.decomprimeAmmissibilita.getOpt)) {
			  decomprime2G(commandLine, args)
				
			} else if (commandLine.hasOption(commandLineOptions.decomprimeGAS.getOpt)) {
				decomprimeGAS(commandLine, args)

			} else if (commandLine.hasOption(commandLineOptions.injection.getOpt) ) {
			  injection(commandLine, args)
			  
			}else if (commandLine.hasOption(commandLineOptions.injection_new.getOpt) ) {
				injection_new(commandLine, args)

			} else if (commandLine.hasOption(commandLineOptions.goAmmissibilita.getOpt)){
        goammissibilita(commandLine, args)
			}else if (commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt)) {
				it.au.misure.aggregazioni.AggregazioneMisureOrarieValidazioni.main(args)
				
			} else if (commandLine.hasOption(commandLineOptions.aggregatiOrari2.getOpt)) {
				it.au.misure.aggregazioni.AggregazioneMisureOrarieValidazioni_2.main(args)

			} /*else if (commandLine.hasOption(commandLineOptions.aggregatiOrari3.getOpt)) {
				it.au.misure.aggregazioni.AggregazioneMisureOrarieValidazioni.main(args)

			}*/else if (commandLine.hasOption(commandLineOptions.aggregatiDettaglio.getOpt)) {
		    it.au.misure.aggregazioni.AggregazioneMisureOrarieDettaglio.main(args)

	    } else if (commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)) {
				it.au.misure.aggregazioni.AmMisureMaster.main(args)
				
			} else if (commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)) {
				injection(commandLine, args)

			}  else if ((commandLine.hasOption(commandLineOptions.commitIN_fromtest_toprod.getOpt) ||
				commandLine.hasOption(commandLineOptions.commitAG_fromtest_toprod.getOpt) ||
				commandLine.hasOption(commandLineOptions.commitAL_fromtest_toprod.getOpt))) {
				runCommit(commandLine, args)

			} else if (commandLine.hasOption(commandLineOptions.test.getOpt)) {
				it.au.misure.util.Test.main(args)

			} else if (commandLine.hasOption(commandLineOptions.rigenera_flusso_misure_quarti.getOpt)) {
				it.au.misure.util.Rigenerate_flusso_misure_quarti.main(args)

			} else if (commandLine.hasOption(commandLineOptions.rigenera_flusso_misure_noaggr.getOpt)) {
				it.au.misure.util.Rigenerate_flusso_misure_noaggr.main(args)

			}else if (commandLine.hasOption(commandLineOptions.sem1.getOpt) || commandLine.hasOption(commandLineOptions.sem2.getOpt)) {
				runSEM(commandLine,args)

			}else if (commandLine.hasOption(commandLineOptions.sem_importOraclePod_F.getOpt) || commandLine.hasOption(commandLineOptions.sem_importOraclePod_O.getOpt) ||  commandLine.hasOption(commandLineOptions.sem_importOracleAll.getOpt) || commandLine.hasOption(commandLineOptions.sem_importOracleDati.getOpt)) {
				runImportRCU(commandLine,args)

			}else if (commandLine.hasOption(commandLineOptions.portale_consumi.getOpt) ||  commandLine.hasOption(commandLineOptions.portale_consumi_export.getOpt)) {
				runEstrazione_ExportPortaleConsumi(commandLine,args)

			}else {
				log.info("Nessun comando specificato! Usa --help per vedere l'uso.")
				printHelpAndExit()
				System.exit(1)
			}
			
			val fine = System.currentTimeMillis() / 1000
			log.info(s"***** tempo esecuzione ${((fine - inizio) / 60)}:${((fine - inizio) % 60)}" )
	}
	
	/**
	 * azioni di utilita'
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 */
	def utility(commandLine:CommandLine) = {
	  // handle version & help
		if (commandLine.hasOption(commandLineOptions.version.getOpt)) {
			printVersionAndExit()
		} else if (commandLine.hasOption(commandLineOptions.help.getOpt)) {
			printHelpAndExit()
		} 

		// enable debug if verbose was specified
		if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
		}
	}
	
	/**
	 * Funzione per la decompressione dei file di misura.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
	 */
	def decomprime2G(commandLine:CommandLine, args: Array[String]) :Boolean = {
	    val tipoFile:String = if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			    "1G"
			  }else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			    "2G"
			  }else{
			    val msg = "Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso."
			    log.info(msg)
			    printHelpAndExit()
			    System.exit(1)
			    msg
			  }
			  log.info("CommandLineOptions.decomprime " + tipoFile)
				it.au.misure.ingestione.Decomprime12G.main(args)
		    return it.au.misure.ingestione.Decomprime12G.rtv_val
	}

	def decomprimeGAS(commandLine:CommandLine, args: Array[String]) :Boolean = {
		log.info("CommandLineOptions.decomprime GAS")
		it.au.misure.ingestione.Decomprime_Gas.main(args)
		return it.au.misure.ingestione.Decomprime_Gas.rtv_val
	}
	
		/**
	 * Funzione per l'ingestione dei file di misura.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
	 */
	def injection(commandLine:CommandLine, args: Array[String]) = {
		if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			log.info("CommandLineOptions.injection1G")
		} else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			log.info("CommandLineOptions.injection2G")
		} else {
			log.info("Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso.")
			printHelpAndExit()
			System.exit(1)
		}
			//it.au.misure.ingestione.InjectionNew.main(args)
		it.au.misure.ingestione.InjectionNew_NoCheckAmm.main(args)
	}

	def injection_new(commandLine:CommandLine, args: Array[String]) = {
		if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			log.info("CommandLineOptions.injection1G")
		} else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			log.info("CommandLineOptions.injection2G")
		} else {
			log.info("Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso.")
			printHelpAndExit()
			System.exit(1)
		}
		it.au.misure.ingestione.InjectionNew.main(args)
	}

	def goammissibilita(commandLine:CommandLine, args: Array[String]) = {
		if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			log.info("CommandLineOptions.ammissibilita 1G")
		} else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			log.info("CommandLineOptions.ammissibilita 2G")
		} else {
			log.info("Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso.")
			printHelpAndExit()
			System.exit(1)
		}

		it.au.misure.ingestione.CheckAmmissibilita.main(args)
	}

	def runCommit(commandLine:CommandLine, args: Array[String]) = {
		if(commandLine.hasOption(commandLineOptions.commitIN_fromtest_toprod.getOpt)) {
			log.info("CommandLineOptions.Commit.Ingestione")
			it.au.misure.commit.Commiter.main(args,it.au.misure.commit.TypeCommit.Ingestione)
		}else if(commandLine.hasOption(commandLineOptions.commitAG_fromtest_toprod.getOpt)) {
			log.info("CommandLineOptions.Commit.Aggregato")
			it.au.misure.commit.Commiter.main(args,it.au.misure.commit.TypeCommit.Aggregati)
		}else{
			log.info("CommandLineOptions.Commit.Tutto")
			it.au.misure.commit.Commiter.main(args,it.au.misure.commit.TypeCommit.Tutto)
		}

	}

	/**
		* Funzione per l'avvio della procedura di rettifica aggregato (SEM 1 e 2).
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @param args argomenti passati da riga di comando
		*/
	def runSEM(commandLine:CommandLine, args: Array[String]) = {

		if(commandLine.hasOption(commandLineOptions.sem1.getOpt)) log.info("CommandLineOptions.SEM 1") else log.info("CommandLineOptions.SEM 2")
		it.au.misure.sem.RettificaAggregazioneSEM.main(args)
	}

	/**
		* Funzione per l'avvio della procedura di importazione dati da Oracle.
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @param args argomenti passati da riga di comando
		*/
	def runImportRCU(commandLine:CommandLine, args: Array[String]) = {

		log.info("CommandLineOptions Import da RCU")
		val rcui=new it.au.misure.sem.RCU_DataImport()
		rcui.init(args)
	}

	def runEstrazione_ExportPortaleConsumi(commandLine:CommandLine, args: Array[String]) = {

		log.info("CommandLineOptions Elaborazione Portale Consumi")
		if(commandLine.hasOption(commandLineOptions.portale_consumi.getOpt))
			it.au.misure.portale.consumi.Estrazione_Misure_EE_new.main(args)
		else it.au.misure.portale.consumi.DataExportToMongoDB.main(args)
	}



	/*
	 * local[*]
	 * yarn-client
	 */
	def execute(commandLine: CommandLine, mainClass: String): Unit = {

	   val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
				"local[*]" 
			}else{
			  "yarn-client"
			}
	  
			val spark = new SparkLauncher()
					.setAppName("Flusso Misure Inserimento Misure Quarti Tool")
					.setAppResource(SparkLauncher.SPARK_MASTER)
					.setMainClass("it.au.misure.util.Decomprime2G")
					.setMaster(master)
					
					for(e <- commandLine.getOptions) {
					  if(e.getValue == null){
					    spark.addAppArgs("--" + e.getLongOpt)
					  }else{
					     spark.addAppArgs("--" + e.getLongOpt, e.getValue)
					  }
					}
					
					println("startApplication start")
					val handle  = spark.startApplication()

					val countDownLatch = new CountDownLatch(1);

			val listener = new SparkAppHandle.Listener {
				override def infoChanged(handle: SparkAppHandle): Unit = {}
				override def stateChanged(handle: SparkAppHandle): Unit = {

						if (handle.getState().isFinal()) {
							countDownLatch.countDown();
						}

				}
			}
	}

}