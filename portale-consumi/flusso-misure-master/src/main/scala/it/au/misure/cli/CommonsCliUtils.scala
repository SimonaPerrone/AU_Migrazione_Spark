package it.au.misure.cli

import java.io.PrintWriter
import java.util.{Calendar, GregorianCalendar, TimeZone}
import java.util.Properties

import scala.util.{Failure, Success, Try}
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.util.{CreateProperties, LoggingSupport}
import org.apache.log4j.{Level, LogManager}


object TypeDataToElab extends Enumeration {
	val Pdo = Value("PDO")
	val Rfo = Value("RFO")
	val Smis = Value("SMIS")
	val Other_Data = Value("ALTRI_FLUSSI")
}
object TypeDataImportRCU extends Enumeration {
	val NONE = Value(0)
	val POD_FILTER = Value(1)
	val MISURE_1718 = Value(2)
	val AGGR_ORA_SEM = Value(3)
	val ALL = Value(4)
}




/**
 * Command line utils leveraging Apache Commons CLI
 */


@SerialVersionUID(114L)
class CommonsCliUtils  extends Serializable{
  
  /**
   * Contiene le proprietà del file job.properties
   */
  val prop:Properties = new CreateProperties(System.getProperty("user.dir")).prop

	def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess

  /**
   * Legge e acquisisce le opzioni passate a riga di comando.
   * @param args argomenti passati
   * @param options rappresenta le opzioni disponibili.
   * @return oggetto che gestisce le varie opzione previste.
   */
	def parseArgsList(args: Array[String], options: Options): CommandLine = {
    
			Try {
				new DefaultParser().parse(options, args)
			} match {
			case Success(settings) => settings
			case Failure(e) => printHelpForOptions(options); throw e
			}
	}

  /**
   * In caso di errata opzione, mostra a video le opzioni disponibili.
   * @param options rappresenta le opzioni disponibili.
   */
	def printHelpForOptions(options: Options) {
		val f = new HelpFormatter()
				f.setWidth(100)
				f.printHelp("flusso-misure", "", options, "", true)
	}

	/**
	 * Utilità per la definizione dei giorni richiesti al processo.
   * @param commandLine oggetto che gestisce le varie opzione previste.
   * @param commandLineOptions contiene le varie opzioni previste.
   * @return List elenco dei giorni. 
	 */
	def getGiorni(commandLine: CommandLine, commandLineOptions:CommandLineOptions): List[String] = {
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val giorni:String = commandLine.getOptionValue(commandLineOptions.giorni.getOpt)
	   val mese:String = if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.mese.getOpt)
	   }else{
	     null
	   }
	   val ret:List[String] = getGiorni(mese, giorni)
	  ret
	}

/**
 * Genera gli argomenti utilizzati in fase di decompressione e ingestione.
 * @param commandLine oggetto che gestisce le varie opzione previste.
 * @param commandLineOptions contiene le varie opzioni previste.
 * @param giornoIn giorno per il quale creare gli argomenti necessari.
 * @return Args argomenti.
 */
def getArgsRange(commandLine: CommandLine, commandLineOptions:CommandLineOptions, giornoIn:String): Args = {
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   
	  try{
	   if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	     calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	   }
	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     //i mesi iniziano da 0: gen=0, feb=1,...
	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	     calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	   }
	     
	   cal.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
	   calXml.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
	    
	  }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	  }

	   
	   val annomesegiornodir:Int =  ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt

	   val nomeFile = {
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 )
	   }
	   
	   val nomeFile2G = {
	     //prendo solo i file del giorno precedente
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + ("0" + calXml.get(Calendar.DAY_OF_MONTH) takeRight 2 )
	   }



		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 val giorno:String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2
     
		 Args(anno, mese, giorno, null, null, null, null, null,  null, nomeFile, nomeFile2G, null, null, annomesegiornodir,TypeDataImportRCU.NONE,false,null,false,false,true)
}

/**
 * Genera gli argomenti utilizzati in fase di aggregazione.
 * @param commandLine oggetto che gestisce le varie opzione previste.
 * @return Args argomenti
 */
def getArgsAggregati(commandLine: CommandLine): Args = {
  
    val commandLineOptions = new CommandLineOptions()
    
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   //val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
//	   val calAggr = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   
	  try{
	   if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	     //calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	   }
	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     //i mesi iniziano da 0: gen=0, feb=1,...
	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1)
       //BUG LIBRERIA
			 val tmp=Integer.toString(cal.get(Calendar.MONTH) + 1)
       cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1)
	    // calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	   }
	     
	    
	  }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	  }

	   
	    val annomesegiornodir:Int = if (commandLine.hasOption(commandLineOptions.annomesegiornodir.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.annomesegiornodir.getOpt).toInt
	   }else{
	     ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt
	   }
	   

		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 
		 val appName:String = if(commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt)|| commandLine.hasOption(commandLineOptions.aggregatiOrari2.getOpt)|| commandLine.hasOption(commandLineOptions.aggregatiOrari3.getOpt) ){
			 "Flusso Misure Aggregazione Misure Orarie"
		 }else if(commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)){
			 "Flusso Misure Aggregazione Am Misure Master"
		 }else{
			 "X"
		 }
	   
		 val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			 "local[*]"
		 }else{
			 "yarn-client"
		 }
     
		 Args(anno, mese, null, null, appName, null, null, null, master, null, null, null, null, annomesegiornodir,TypeDataImportRCU.NONE,false,null,false,false,true)
}

	/**
		* Genera gli argomenti utilizzati per ottenere il dettaglio pod a seguito di aggregato orario
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @return Args argomenti
		*/
	def getArgsDettaglioPodAggregati(commandLine: CommandLine): Args = {

		val commandLineOptions = new CommandLineOptions()

		val timeZone = prop.getProperty("spark.app.time_zone")
		val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
		val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));


		try{
			if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
				cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
				calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
			}	else throw new Exception("")

			if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
				//i mesi iniziano da 0: gen=0, feb=1,...
				cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
				calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
			}	else throw new Exception("")



			//val versione_orarie:Long = commandLine.getOptionValue(commandLineOptions.aggregatiDettaglio.getOpt).toLong

		}catch{
			case e: Exception => throw new Exception("Inseriti valori non validi verificare anno,mese", e)
		}





		val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2

		val appName:String = if(commandLine.hasOption(commandLineOptions.aggregatiDettaglio.getOpt)){
			"Estrazione dettaglio Pod Da Aggregato Orario"
		}else{
			"X"
		}

		val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			"local[*]"
		}else{
			"yarn-client"
		}

		Args(anno, mese, null, null, appName, null, null, null, master, null, null, null, null, 0,TypeDataImportRCU.NONE,false,null,false,false,true)
	}
	/**
		* Genera gli argomenti utilizzati in fase di sem(rettifica aggregato).
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @return Args argomenti
		*/
	def getArgsSEM(commandLine: CommandLine): Args = {

		val commandLineOptions = new CommandLineOptions()

		val timeZone = prop.getProperty("spark.app.time_zone")
		val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));




		val tipo_sem:String = if(commandLine.hasOption(commandLineOptions.sem1.getOpt))"SEM1" else "SEM2"

		val sessione:String=if(commandLine.hasOption(commandLineOptions.sem_session.getOpt)){
			val tmp =commandLine.getOptionValue(commandLineOptions.sem_session.getOpt).trim
			if(tmp!="S1" && tmp!="S2")
				""
			else
				tmp
		}
		else ""



		val appName:String = if(commandLine.hasOption(commandLineOptions.sem1.getOpt)){
			if(commandLine.hasOption(commandLineOptions.sem_commit.getOpt))
				"Commit Rettifica Aggregato Misure EE 1(SEM 1)"
			else{
				if(commandLine.hasOption(commandLineOptions.sem_invalidazioni.getOpt) && sessione !="")
					"Rettifica Aggregato Misure EE 1(SEM 1) + Invalidazioni"
				else if(commandLine.hasOption(commandLineOptions.sem_invalidazioni.getOpt) && sessione =="")
					"Invalidazioni (SEM 1)"
				else "Rettifica Aggregato Misure EE 1(SEM 1)"
			}
		}else if(commandLine.hasOption(commandLineOptions.sem2.getOpt)){
			if(commandLine.hasOption(commandLineOptions.sem_commit.getOpt))
				"Commit Rettifica Aggregato Misure EE 2(SEM 2)"
			else {
				if(commandLine.hasOption(commandLineOptions.sem_invalidazioni.getOpt) && sessione !="")
					"Rettifica Aggregato Misure EE 1(SEM 2) + Invalidazioni"
				else if(commandLine.hasOption(commandLineOptions.sem_invalidazioni.getOpt) && sessione =="")
					"Invalidazioni (SEM 2)"
				else "Rettifica Aggregato Misure EE 1(SEM 2)"
			}
		}else{
			"X"
		}

	 if(commandLine.hasOption(commandLineOptions.sem_force_anno_start.getOpt)) {
		val tmp = commandLine.getOptionValue(commandLineOptions.sem_force_anno_start.getOpt)
		if (!isInt(tmp))
			throw new Exception("Attenzione l'anno start della SEM passato tramite parametro non è un numero valido!")
		else cal.set(Calendar.YEAR, tmp.toInt)
	 }

		val anno:String = 	if(tipo_sem=="SEM1") Integer.toString(cal.get(Calendar.YEAR))
												else  Integer.toString(cal.get(Calendar.YEAR)-1)


		val mese:String = if(tipo_sem=="SEM1")"06"
											else "12"

		val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			"local[*]"
		}else{
			"yarn-client"
		}

		val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
			"DEBUG"
		}else{
			LogManager.getRootLogger.setLevel(Level.ERROR)
			"ERROR"
		}


		val makeImportFromOracle =if (commandLine.hasOption(commandLineOptions.sem_importOraclePod_O.getOpt)){
			TypeDataImportRCU.POD_FILTER
		}else if (commandLine.hasOption(commandLineOptions.sem_importOraclePod_F.getOpt)) {
			TypeDataImportRCU.MISURE_1718
		}else if (commandLine.hasOption(commandLineOptions.sem_importOracleDati.getOpt)) {
			TypeDataImportRCU.AGGR_ORA_SEM
		}else if (commandLine.hasOption(commandLineOptions.sem_importOracleAll.getOpt)) {
			TypeDataImportRCU.ALL
		}else TypeDataImportRCU.NONE


		val isSemTot:Boolean=if (commandLine.hasOption(commandLineOptions.sem_force_tot.getOpt))true else false
		val isSemCommit:Boolean=if (commandLine.hasOption(commandLineOptions.sem_commit.getOpt))true else false

		val findInvalidazioni:Boolean=if (commandLine.hasOption(commandLineOptions.sem_invalidazioni.getOpt))true else false

		val annomesegiornodir:Int = if (commandLine.hasOption(commandLineOptions.annomesegiornodir.getOpt)) {
			commandLine.getOptionValue(commandLineOptions.annomesegiornodir.getOpt).toInt
		}else 0


		Args(anno, mese, null, tipo_sem, appName, logLevel, null, null, master, null, null, null, null, annomesegiornodir,makeImportFromOracle,isSemTot,sessione,isSemCommit,findInvalidazioni,true)
	}

	/**
		* Genera gli argomenti utilizzati in fase di sem(rettifica aggregato).
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @return Args argomenti
		*/
	def getArgsImportRCU(commandLine: CommandLine): Args = {

		val commandLineOptions = new CommandLineOptions()

		val timeZone = prop.getProperty("spark.app.time_zone")
		val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));



		val appName:String = if(commandLine.hasOption(commandLineOptions.sem_importOraclePod_O.getOpt)){
			"Import RCU-Cloudera POD ORARI 2018 "
		}else if(commandLine.hasOption(commandLineOptions.sem_importOraclePod_F.getOpt)) {
			"Import RCU-Cloudera MISURE GEN 17- LUG 18 "
		}else if(commandLine.hasOption(commandLineOptions.sem_importOracleDati.getOpt)){
			"Import RCU-Cloudera AGGREGATI PERIODICI-SEM "
		}else if(commandLine.hasOption(commandLineOptions.sem_importOracleAll.getOpt)){
			"Import RCU-Cloudera POD ORARI 2018 - QUARTI - AGGREGATI PERIODICI-SEM "
			}else {
			"X"
		}


		val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			"local[*]"
		}else{
			"yarn-client"
		}

		val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
			"DEBUG"
		}else{
			LogManager.getRootLogger.setLevel(Level.ERROR)
			"ERROR"
		}

		val makeImportFromOracle =if (commandLine.hasOption(commandLineOptions.sem_importOraclePod_O.getOpt)){
			TypeDataImportRCU.POD_FILTER
		}else if (commandLine.hasOption(commandLineOptions.sem_importOraclePod_F.getOpt)) {
			TypeDataImportRCU.MISURE_1718
		}else if (commandLine.hasOption(commandLineOptions.sem_importOracleDati.getOpt)) {
			TypeDataImportRCU.AGGR_ORA_SEM
		}else if (commandLine.hasOption(commandLineOptions.sem_importOracleAll.getOpt)) {
			TypeDataImportRCU.ALL
		}else TypeDataImportRCU.NONE




		Args(null, null, null, null, appName, logLevel, null, null, master, null, null, null, null, 0,makeImportFromOracle,false,null,false,false,true)
	}

	/**
		* Genera gli argomenti utilizzati per le estrazione utilizzati dal portale dei consumi.
		* @param commandLine oggetto che gestisce le varie opzione previste.
		* @return Args argomenti
		*/
	def getArgsPort_Consumi(commandLine: CommandLine): Args = {

		val commandLineOptions = new CommandLineOptions()

		/*val timeZone = prop.getProperty("spark.app.time_zone")
		val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
    */

		var appName :String =""
		var tipoEstrazione_Export:String =""
		var tipo_estrazione_notset=false

		if(commandLine.hasOption(commandLineOptions.portale_consumi.getOpt)) {
			tipoEstrazione_Export = commandLine.getOptionValue(commandLineOptions.portale_consumi.getOpt).trim

			appName = if (commandLine.hasOption(commandLineOptions.portale_consumi.getOpt)) {
				"PORTALE_CONSUMI"
			} else {
				"X"
			}
		}else if(commandLine.hasOption(commandLineOptions.portale_consumi_export.getOpt)) {
			tipoEstrazione_Export = commandLine.getOptionValue(commandLineOptions.portale_consumi_export.getOpt).trim

			appName = if (commandLine.hasOption(commandLineOptions.portale_consumi_export.getOpt)) {
				"PORTALE_CONSUMI-ESPORTAZIONE MISURE SU MONGODB"
			} else {
				"X"
			}
		}
		else {
			appName=""
			tipoEstrazione_Export=""
			tipo_estrazione_notset=true
		}



		val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			"local[*]"
		}else{
			"yarn-client"
		}

		val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
			"DEBUG"
		}else{
			LogManager.getRootLogger.setLevel(Level.ERROR)
			"ERROR"
		}

		val numMesiElab:String= if(commandLine.hasOption(commandLineOptions.numMesi_PortaleConsumi.getOpt)) commandLine.getOptionValue(commandLineOptions.numMesi_PortaleConsumi.getOpt).trim else "0"


		Args(null, numMesiElab, null, tipoEstrazione_Export, appName, logLevel, null, null, master, null, null, null, null, 0,null,false,null,false,false,tipo_estrazione_notset)
	}

	def getArgs_Gas(commandLine: CommandLine, giornoIn:String=""): Args = {
		val commandLineOptions = new CommandLineOptions()

		val timeZone = prop.getProperty("spark.app.time_zone")

		val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
		val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));



		try{
			if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
				cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
				calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
			}
			if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
				//i mesi iniziano da 0: gen=0, feb=1,...
				cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
				calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
			}
			if (commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
				cal.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
				calXml.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
			}
			else if (giornoIn!="") {
				cal.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
				calXml.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
			}else{
				// in tutti i casi devo prendere la cartella del giorno precedente di quando parte il processo
				cal.add(Calendar.DAY_OF_MONTH, -1)
				calXml.add(Calendar.DAY_OF_MONTH, -1)
			}
		}catch{
			case e: Exception => throw new Exception("Inseriti valori non validi", e)
		}



		val nomeFile = {
			"" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 )
		}

		val nomeFile2G = {
			//prendo solo i file del giorno precedente
			"" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + ("0" + calXml.get(Calendar.DAY_OF_MONTH) takeRight 2 )
		}


		val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		val giorno:String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2
		var tipo_flusso_notset=false

		val tipo_flusso:String = if (commandLine.hasOption(commandLineOptions.decomprimeGAS.getOpt)) {
			val tmp = commandLine.getOptionValue(commandLineOptions.decomprimeGAS.getOpt)
			if(tmp == null)
			{tipo_flusso_notset=true
			""}else
				tmp.toUpperCase()
		}else {
			tipo_flusso_notset=true
			""
		}


		val daelenco:Boolean=if(tipo_flusso.contains("-R"))true else false

		 val appName:String =  if(commandLine.hasOption(commandLineOptions.decomprimeGAS.getOpt)){
				"Decompressione Misure GAS " + tipo_flusso
			}else{
				" Spark APP"
			}


		/*
     * cartella temporanea dove vengono decompressi i file di misura zippati
     */
		val tmpDir:String = if(commandLine.hasOption(commandLineOptions.decomprimeGAS.getOpt)) {
			if(daelenco)
				prop.getProperty("spark.app.directory.temporanea.GAS_Recupero_Puntuale")
		  else
			  prop.getProperty("spark.app.directory.temporanea.GAS")
		}
		 else{
			""
		}

		/*
     * cartella root dei file di misura zippati
     */
		val rootDir:String = if(commandLine.hasOption(commandLineOptions.decomprimeGAS.getOpt)){
			prop.getProperty("spark.app.directory.root.GAS") // isilonshare
		}else{
			""
		}


		val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
			"DEBUG"
		}else{
			LogManager.getRootLogger.setLevel(Level.ERROR)
			"ERROR"
		}

		val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			"local[*]"
		}else{
			"yarn-client"
		}

		Args(anno, mese, giorno, tipo_flusso, appName, logLevel, tmpDir, rootDir,  master, nomeFile, nomeFile2G, null, null, 0,TypeDataImportRCU.NONE,false,null,false,false,tipo_flusso_notset)
	}

	def getArgs(commandLine: CommandLine, giornoIn:String=""): Args = {
	  val commandLineOptions = new CommandLineOptions()
	  
	   val timeZone = prop.getProperty("spark.app.time_zone")
	  
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   


	   try{
	     if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
  	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
  	     calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
  	   }
  	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
  	     //i mesi iniziano da 0: gen=0, feb=1,...
  	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
  	     calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
  	   }
  	   if (commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
  	     cal.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
  	     calXml.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
  	   }
			 else if (giornoIn!="") {
				 cal.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
				 calXml.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
			 }else{
  	     // in tutti i casi devo prendere la cartella del giorno precedente di quando parte il processo
  	     cal.add(Calendar.DAY_OF_MONTH, -1)
  	     calXml.add(Calendar.DAY_OF_MONTH, -1)
  	   }
	   }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	   }
	   
	   
	   val annomesegiornodir:Int = if (commandLine.hasOption(commandLineOptions.annomesegiornodir.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.annomesegiornodir.getOpt).toInt
	   }else{
	     ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt
	   }

	   
	   val nomeFile = {
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 )
	   }
	   
	   val nomeFile2G = {
	     //prendo solo i file del giorno precedente
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + ("0" + calXml.get(Calendar.DAY_OF_MONTH) takeRight 2 )
	   }
	   

		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 val giorno:String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2
     var pdo_rfo_notset=false

						val pdo_rfo:String = if (commandLine.hasOption(commandLineOptions.pdo.getOpt)){
							TypeDataToElab.Pdo.toString
						}else if (commandLine.hasOption(commandLineOptions.rfo.getOpt)){
							TypeDataToElab.Rfo.toString
						}else if (commandLine.hasOption(commandLineOptions.smis.getOpt)){
							TypeDataToElab.Smis.toString
						}else if (commandLine.hasOption(commandLineOptions.other_f.getOpt)){
							TypeDataToElab.Other_Data.toString
						}else {
							pdo_rfo_notset=true
							TypeDataToElab.Pdo.toString
						}


						val appName:String = if(commandLine.hasOption(commandLineOptions.injection.getOpt) || commandLine.hasOption(commandLineOptions.injection_new.getOpt)){
  						  if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
  							  "Injection Misure 2G " +(if(commandLine.hasOption(commandLineOptions.injection_new.getOpt)) " NEW " else "" ) + pdo_rfo
  						  }else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
  							  "Injection Misure 1G " +(if(commandLine.hasOption(commandLineOptions.injection_new.getOpt)) " NEW " else "" ) + pdo_rfo
  						  }else{
  						    "Injection Misure " +(if(commandLine.hasOption(commandLineOptions.injection_new.getOpt)) " NEW " else "" )
  						  }
  						}else if(commandLine.hasOption(commandLineOptions.decomprime2G.getOpt) || commandLine.hasOption(commandLineOptions.decomprimeAmmissibilita.getOpt)){
							 val descr=if(commandLine.hasOption(commandLineOptions.decomprimeAmmissibilita.getOpt))"(Ammissibilità)" else ""

  						  if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
  							  s"Decompressione Misure 2G${descr} " + pdo_rfo
  						  }else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
  							  s" Decompressione Misure 1G${descr} " + pdo_rfo
  						  }else{
  						    s"Decompressione Misure${descr} " + pdo_rfo
  						  }
  						}else if(commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt) || commandLine.hasOption(commandLineOptions.aggregatiOrari2.getOpt)){
  							"Aggregazione Misure Orarie"
  						}else if(commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)){
  							"Aggregazione Am Misure Master"
  						}else if(commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)){
  							"Aggiornamenti"
  						}else if(commandLine.hasOption(commandLineOptions.test.getOpt)){
  							"Test"
							}else if(commandLine.hasOption(commandLineOptions.commitIN_fromtest_toprod.getOpt)) {
								"Commit dei dati di ingestione dal db di collaudo a db di produzione"
							}else if(commandLine.hasOption(commandLineOptions.commitAG_fromtest_toprod.getOpt)) {
								"Commit dei dati aggregati dal db di collaudo a db di produzione"
							}else if(commandLine.hasOption(commandLineOptions.commitAL_fromtest_toprod.getOpt)) {
								"Commit di tutti dati dal db di collaudo a db di produzione"
							}else if(commandLine.hasOption(commandLineOptions.goAmmissibilita.getOpt)) {
							   if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)) "Ammissibilità misure 1G" else "Ammissibilità misure 2G"
						   }else{
  							"X"
  						}
						
						/*
						 * cartella temporanea dove vengono decompressi i file di misura zippati
						 */
						val tmpDir:String = if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
							prop.getProperty("spark.app.directory.temporanea.2G") // /mnt/isilonshare1/TMP_2G
						}else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
							prop.getProperty("spark.app.directory.temporanea.1G") // /mnt/isilonshare1/TMP_1G_collaudo0720
						}else{
						  ""
						}
						
						/*
						 * cartella root dei file di misura zippati
						 */
						val rootDir:String = if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
							prop.getProperty("spark.app.directory.root.2G") // isilonshare
						}else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
							prop.getProperty("spark.app.directory.root.1G") // Test_clouderaShare
						}else{
						  ""
						}
						 
						
							val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
							  LogManager.getRootLogger.setLevel(Level.DEBUG)
								"DEBUG"
							}else{
							  LogManager.getRootLogger.setLevel(Level.ERROR)
								"ERROR"
							}
							
							val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
								"local[*]"
							}else{
								"yarn-client"
							} 

							Args(anno, mese, giorno, pdo_rfo, appName, logLevel, tmpDir, rootDir,  master, nomeFile, nomeFile2G, null, null, annomesegiornodir,TypeDataImportRCU.NONE,false,null,false,false,pdo_rfo_notset)
	}
	
	def getGiorni(mese:String, giorno:String) : List[String] = {
		val cal = new GregorianCalendar();
		if(mese != null){
		  cal.set(Calendar.MONTH, Integer.parseInt(mese) - 1);
		}
		
		val ags = giorno.split(",");
		
		val maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		val minDay = 1;
		val startsWith = giorno.startsWith(",");
		val endsWith = giorno.endsWith(",");
		val isEmpty = giorno.isEmpty();
		val length = ags.length;
		
		val listaGiorni:List[String] = if(isEmpty){ //nessun valore
		  println("Nessun valore valido")
		  Nil
		}else if(startsWith && endsWith){ // , -> dal primo all'ultimo giorno del mese
			println("da giorno 1 a giorno " + maxDay.toString + " del mese " + mese)
		  val eaValues = (1 to maxDay).map( "0" + _ takeRight 2 )
		  eaValues.toList
		}else if(endsWith){ //dall'ultimo giorno specificato a fine mese
		  val eaValues = (ags( ags.length - 1 ).toInt + 1 to maxDay).map { "0" + _.toString takeRight 2 }.toList
		  ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList ++ eaValues
		}else if(startsWith){ // dal primo giorno del mese al primo giorno specificato
      val eaValues = (1 to (ags(1).toInt - 1)).map { "0" + _.toString() takeRight 2 }.toList
      eaValues ++ ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList
		}else {// prendo i giorni specificati
		  ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList
		}
		
		listaGiorni
  }

case class Args(anno:String, mese:String, giorno:String, PdoRfo:String, appName:String, logLevel:String,
								injectionTmp:String, rootDir:String, master:String, nomeFile:String, nomeFile2G:String,
								meseAggr:String, annoAggr:String, annomesegiornodir:Int,importFromOracleSem:TypeDataImportRCU.Value,
								semCalcTot :Boolean,semsession:String,SemCommit:Boolean,SemInvalidazioni:Boolean,tipoflusso_estrazione_notset:Boolean)


}