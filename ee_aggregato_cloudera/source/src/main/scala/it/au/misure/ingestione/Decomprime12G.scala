package it.au.misure.ingestione

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import collection.mutable.{ArrayBuffer, ListBuffer}
import java.io.File
import java.io.FilenameFilter

import it.au.misure.commons.cli.CommandLine
import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils, TypeDataToElab}
import it.au.misure.util.{CreateProperties, LoggingSupport, ZipArchive}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

import sys.process._
import java.lang.management.ManagementFactory

import scala.util.Try
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import it.au.misure.util.Schemas._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}


/**
 * ==Flusso Misure Decompressione==
 * Decomprime gli archivi 1G/2G su una tabella temporanea. I flussi misura vengo acquisiti in formato zip sotto un’alberatura di sottocartelle predefinita, 
 * quindi la prima fase è la lettura di tutti i file dell’alberatura che devono essere acquisiti.
 * Nella seconda fase i file vengono decompressi in una cartella temporanea mantenendo la stessa alberatura originale per essere successivamente acquisiti dal processo stesso.
 * 
 * 
 */
object Decomprime12G extends LoggingSupport {

	var rtv_val=false


/**
 * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
 * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
 */
var sqlcontx:SQLContext=null
val tipi_di_flusso = List("PDO","PDO2G","PNO","PNO2G","VNO","VNO2G","SNM","SNM2G","RFO","RFO2G","RNO","RNO2G","RNV","RNV2G","RSN","RSN2G","SOF","SOS","SNF","SNS","SMIS")

	def can_go_decompressione(is_ammissibilita:Boolean): Boolean ={
		val vmName = ManagementFactory.getRuntimeMXBean.getName
		val p = vmName.indexOf("@")
		val cur_pid = vmName.substring(0, p)

		val parAmm:String=if(is_ammissibilita)"a" else ""
		//verifica che non ci sia decompressione o ingestione in corso
		val tmpDec = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| s"grep D${parAmm}" !!) getOrElse("")
		val tmpInj = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| s"grep i${parAmm}" !!) getOrElse("")


		var pidfound=""

		if(tmpDec!="" && tmpDec.contains(s".jar -D${parAmm}")&& !tmpDec.contains(s".jar -DGAS") )
		{
			val vals=tmpDec.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					pidfound=vals(i)
					if(cur_pid!=pidfound){
						val descr = if(is_ammissibilita)"decompressione ammissibilità" else "decompressione"

						log.info(s"Attenzione è stato trovato un processo di ${descr} in corso , attendere la fine del processo in corso e riprovare")
						log.info(s"Utente processo ${descr} in corso : " + utente)
						log.info(s"PID processo ${descr} in corso : " + pidfound)
						return false
					}

				}
			}

		}

		pidfound=""
		if(tmpInj!="" && tmpInj.contains(s".jar -i${parAmm}"))
		{
			val vals=tmpInj.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					val pid=vals(i)
					if(cur_pid!=pid) {
						val descr = if(is_ammissibilita)"ammissibilità" else "ingestione"

						log.info(s"Attenzione è stato trovato un processo di ${descr} in corso , attendere la fine del processo in corso e riprovare")
						log.info(s"Utente processo ${descr} in corso : " + utente)
						log.info(s"PID processo ${descr} in corso : " + pidfound)
						return false
					}

				}
			}

		}

		return true
	}

  def walkTree(file:File):Iterable[File]={
    val children = new Iterable[File]{
      def iterator = if(file.isDirectory) file.listFiles.iterator else Iterator.empty
    }
    Seq(file)++: children.flatMap(walkTree(_))
  }

  def getCountFiles(dir:File): Long= {
    var count:Long =0
    for (f <- walkTree(dir)) if(f.getName.toLowerCase.matches(".*\\.xml")) count =count + 1

    return count
  }

	def main(args: Array[String]) {


		val commonsCliUtils = new CommonsCliUtils()
		val commandLineOptions = new CommandLineOptions()
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = commonsCliUtils.getArgs(commandLine)

		println("*** anno: " + argsObjMaster.anno.toString)
		println("*** mese: " + argsObjMaster.mese.toString)

    val tipo_flusso_tmp: String = argsObjMaster.PdoRfo
    val verbose:Boolean = if(tipo_flusso_tmp.endsWith("-V"))true else false
    val pdo_rfo: String = tipo_flusso_tmp.replace("-V","")
    val daelenco:Boolean=if(tipo_flusso_tmp.contains("-R"))true else false
    val test:Boolean = if(tipo_flusso_tmp.contains("-T"))true else false

		val isGiornoSingolo: Boolean = if (commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
			println("*** giorno: " + argsObjMaster.giorno)
			true
		} else {
			false
		}

    val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())

		val giorniList: List[String] = if (isGiornoSingolo) {
			List("giorno_singolo")
		} else {
			commonsCliUtils.getGiorni(commandLine, commandLineOptions)
		}
		if (!isGiornoSingolo)
			println("*** giorni: " + giorniList.toString())



     val is_ammissibilita=commandLine.hasOption(commandLineOptions.decomprimeAmmissibilita.getOpt)
		if(!can_go_decompressione(is_ammissibilita)) return

		if(is_ammissibilita){

			if (!commandLine.hasOption(commandLineOptions.anno.getOpt) || !commandLine.hasOption(commandLineOptions.mese.getOpt) || !commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
				log.info("**** Bisogna specificare l'anno il mese e il giorno da decomprimere per la fase di ammissibilità")
				return
			}
			println("***** Inizio processo Decompressione Misure Ammissibilità *****")
		} else
			println("***** Inizio processo Decompressione Misure *****")

		val nameApp = if (argsObjMaster.appName.contains("Decompressione ed ingestione")) {
			log.info("***** Fase di decompressione *****")
			argsObjMaster.appName + " - Fase di decompressione "
		} else {
			argsObjMaster.appName
		}

		println("***** current user " + System.getProperty("user.name") + "****")
		val propertiesC = new CreateProperties(System.getProperty("user.dir"))
		println(propertiesC.printEnvVar)

   val modalitaMaster="local[*]" // argsObjMaster.master
		val conf = new SparkConf()
			.setAppName(nameApp)
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.kryoserializer.buffer.max", "1024")

			.setMaster( modalitaMaster )

		val sc = new SparkContext(conf)
		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
		sc.hadoopConfiguration.setInt("parquet.block.size", blocksize)
		sc.hadoopConfiguration.setInt("dfs.blocksize", blocksize)
		sc.setLogLevel(argsObjMaster.logLevel)

		//	val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

		var sqlCtx = new HiveContext(sc)
		sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
		sqlCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
		sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
		sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
		sqlCtx.setConf("hive.exec.dynamic.partition", "true")
		sqlCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

		sqlCtx.setConf("spark.sql.parquet.mergeSchema", "false")
		sqlCtx.setConf("spark.sql.parquet.filterPushdown", "true")
		sqlCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")

		try {

      val slash = sc.getConf.get("spark.flusso.misure.slash")
      val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions").toInt
      val injectionTmp: String = if (is_ammissibilita) argsObjMaster.injectionTmp + "_ammissibilita" else argsObjMaster.injectionTmp
      val rootDir: String = argsObjMaster.rootDir


      val injection: String = if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
        sc.getConf.get("spark.flusso.misure.injection1G").concat(rootDir).concat(File.separator)
      } else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
        sc.getConf.get("spark.flusso.misure.injection2G").concat(rootDir).concat(File.separator)
      } else {
        commonsCliUtils.printHelpForOptions(commandLineOptions.getOptions)
        System.exit(0)
        ""
      }

      val isAggiorna: Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)

      //println("*** sc.master: " + sc.master)
      println("*** sc.sparkUser: " + sc.sparkUser)
      println("*** injection: " + injection)
      println("*** injectionTmp: " + injectionTmp)
      println("*** rootDir: " + rootDir)
      println("*** minPartitions: " + minPartitions)


      if (is_ammissibilita && argsObjMaster.tipoflusso_estrazione_notset)
        println("*** pdo_rfo_altro: tutti i tipi di flusso")
      else
        println("*** pdo_rfo_altro: " + pdo_rfo)

      println("*** isAggiorna: " + isAggiorna)

      val quarti = sc.getConf.get("spark.flusso.misure.quarti")
      val reports = sc.getConf.get("spark.flusso.misure.report")
      val m_orarie = sc.getConf.get("spark.aggregazioni.misure.orarie")
      val m_am = sc.getConf.get("spark.aggregazioni.misure.am")

      val report_decompressione =  sc.getConf.get("spark.flusso.misure.ee.reportdecompressione") + (if(test) "_test" else "") //Try(sc.getConf.get("spark.flusso.misure.ee.reportdecompressione")).getOrElse("/user/hive/warehouse/au.db/misure_ee_au/report_decompressione")

      println("*** Verifica file di scrittura su hdfs ")
      println("*** percorso flusso_misure_quarti: " + quarti)
      println("*** percorso aggregazioni_misure_orarie: " + m_orarie)
      println("*** percorso aggregazioni_misure_am: " + m_am)
      println("*** percorso report_ingestione: " + reports)
      println("*** percorso report decompressione: " + report_decompressione)

      if (report_decompressione == "") {
        log.info("ERRORE: bisogna impostare il percorsa di scrittura relativo al report di decompressione")
        return
      }
      println(s"*** timestamp di decompressione: ${dataelaborazione}")


      val tmpDirClean = new File(s"${injectionTmp}${File.separator}${rootDir}")
      if (!tmpDirClean.exists()) tmpDirClean.mkdirs()


      if (tmpDirClean.exists() && tmpDirClean.isDirectory() && !tmpDirClean.listFiles().isEmpty) {


        val tmpDirList = tmpDirClean.listFiles().map(xml => xml.getPath).toList
        val oldFiles = sc.parallelize(tmpDirList, 20).setName("Scansiona alberatura temporanea")
        val rddx = oldFiles.map(xmlDir => delete(new File(xmlDir))).setName("Cancella alberatura temporanea").collect()
        val notdeleted = rddx.filter(p => p == false)
        if (notdeleted.length > 0) {
          val oldFiles2 = sc.parallelize(tmpDirList, 5)
          val rddx2 = oldFiles2.map(xmlDir => delete(new File(xmlDir))).collect()
          val notdeleted2 = rddx2.filter(p => p == false)
          if (notdeleted2.length > 0) {
            log.info(s"Alcuni file presenti nella cartella temporanea non sono stati cancellati. Riprova oppure esegui una cancellazione manuale con il comando rm -R -f ${injectionTmp}")
            rtv_val = false
            return
          }
        }


      }
      if (tmpDirClean.exists())
        tmpDirClean.delete()

      println("*** pulizia della cartella temporanea prima della scompattazione OK")
      writeDataElabEE_HDFS(true,dataelaborazione)

      println("*** controllo della cartella temporanea OK")

      println("*** Avvio Scansione alberatura per UDD")

      val elencoUDD = scansionaUDD(injection).toSeq

      val rddUDD = sc.parallelize(elencoUDD).setName("Scansiona alberatura")
      rddUDD.cache()
      println("*** scansiona alberatura UDD OK ")

      val numfdec: org.apache.spark.Accumulator[Int] = sc.accumulator(0, "Files_decompressi")
      var listFiles = ListBuffer[Row]()

      if (modalitaMaster.equals("local[*]")) {


        var zF = sc.broadcast(listFiles)

       val numthread = 100


        val annomese_files = (argsObjMaster.anno + argsObjMaster.mese).toInt

        rddUDD.foreach { uddDir =>
          // println(s"${uddDir}")
          // println(s"${giorniList}")


          var list = new ArrayBuffer[Thread]()


          val ret = leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions, zF, dataelaborazione, annomese_files)
          ret.foreach(filePath => {
            val t = new Thread {

              override def run {
                decomprimiAlberatura(filePath, rootDir, injectionTmp, verbose, zF, dataelaborazione, annomese_files)

              }
            }

            list.append(t)

            t.start()

						if(list.length>=numthread)
						{
							var exited=0
							breakable {
								for (el_t <- list) {
									if (el_t != null && el_t.isAlive) {
										el_t.join()
										exited+=1
									}else exited+=1

									if(exited>=10)
										break
								}
							}

							list=list.filter(t => t!=null && t.isAlive)

						}

          })


          var cicla=true
          var all_exited=true

          while(cicla){
            all_exited=true

            for (el_t <- list){
              if(el_t!=null && el_t.isAlive)
              {
                all_exited=false
                el_t.join()
              }
            }
            if(!all_exited)
              Thread.sleep(2000)
            else
              cicla=false
          }
        }

      }
      /*else {
			 rddUDD.foreach { uddDir =>
				 // println(s"${uddDir}")
				 // println(s"${giorniList}")
				 val ret = leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions)
				 ret.foreach(filePath => {
					 val rtc = decomprimiAlberatura(filePath, rootDir, injectionTmp)
					 if (rtc == "")
					  throw new Exception("Attenzione il file : " + filePath + " non è stato decompresso poichè non è possibile leggerlo")
				 })
			 }
			}*/

      log.info("*** Avvio processo verifica file xml decompressi")
			val tmpDirPath = injectionTmp.concat(File.separator).concat(rootDir)
			val totF=getCountFiles(new File(tmpDirPath))

			log.info(s"**** Tot files scritti nel path ${tmpDirPath} :" + totF.toString +" ****")


      /*rddUDD.foreach { uddDir =>
        val ret = leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions)
        var dictfiles_udd: Map[String, String] = Map("" -> "")
        ret.foreach(filePath => {
          val dirPath = filePath.substring(filePath.lastIndexOf(rootDir), filePath.lastIndexOf(File.separator))
          val tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)

          if (!dictfiles_udd.contains(tmpDirPath)) {

            val list = new File(tmpDirPath).listFiles
						if(list!=null)
            numfdec += list.length

            dictfiles_udd += (tmpDirPath -> "ok")
          }

        })
        dictfiles_udd.empty
        dictfiles_udd = null
      }


      log.info("*** Numero di file decompressi : " + numfdec.value.toString)


			if(totF!=numfdec.value)
				log.info(s"*** ATTENZIONE SI E' VERIFICATA UNA INCONGUENZA TRA IL NUMERO DI FILES SCRITTI ED IL NUMERO DI FILES ELABORATI IN BASE AI PARAMETRI IN INGRESSO : ${totF} <> ${numfdec.value}. VERIFICARE IL PRIMA POSSIBILE")
      */
      writeDataElabEE_HDFS(false,dataelaborazione)
			if (listFiles.length != totF) {
        log.info(s"*** SI E' VERIFICATA UNA INCONGUENZA TRA I FILE DECOMPRESSI E L'ELECO DEI FILE DA SCRIVERE SU HIVE : ${totF} <> ${listFiles.length}")

				log.info("*** Avvio controllo sull'incongruenza")
				var counter=0
				for (el:Row <- listFiles){
					val str=el.getString(3)
					val f = new File(str)

					if(!f.exists()) {
						val name_upper_case=f.getName.toUpperCase
						val has_flusso=tipi_di_flusso.exists(name_upper_case.contains(_))

						if((!has_flusso) && ( name_upper_case.endsWith(".ZIP")  ))
							log.info(s"Il file ${f.getName} non verrà elaborato poichè il nome file non ha un tipo flusso che ricade tra i flussi previsti : ${tipi_di_flusso.toString}")
						else
						  log.info(s"Il file : ${str} non è presente nella cartella di decompressione")

						counter=counter+1
					}
					else if(f.isDirectory) {
						counter = counter + 1
						val f2= new File(el.getString(2))
						val name_upper_case=f2.getName.toUpperCase
						val has_flusso=tipi_di_flusso.exists(name_upper_case.contains(_))

						if((!has_flusso) && ( name_upper_case.endsWith(".ZIP")  ))
							  log.info(s"Il file ${f2.getName} non verrà elaborato poichè il nome file non ha un tipo flusso che ricade tra i flussi previsti : ${tipi_di_flusso.toString}")
						else
						    log.info("File sorgente :" + el.getString(2) + " non decompresso /copiato")
					}
				}
				if(counter==0)
					log.info(s"*** L'INCONGRUENZA SULLA NUMEROSITA E' DOVUTA A FILE SORGENTI DUPLICATI - VERIFICARE LA TABELLA report_decompressione SU HIVE")

			}


      val rddToWrite = sc.parallelize(listFiles.toList)

      val dfQS3 = sqlCtx.createDataFrame(rddToWrite, schemaReportDecompressione)

      log.info("***** creazione DataFrame Report misure elettriche decompresse OK")

      dfQS3
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("annomese")
        .save(report_decompressione) //"/user/silvia/au/misure_ee_au/report_decompressione")

      sqlCtx.sql("MSCK REPAIR TABLE au.report_decompressione"+ (if(test) "_test" else ""))



      println("*** decomprimi alberatura OK")

      rtv_val = true

    } catch {
			case e: Exception => {
				log.info(e.getMessage)
				rtv_val = false
			}
		} finally {
			sc.stop()

		}

		println("***** Fine processo Decompressione Misure *****")

	}


	
	def pri(udd:String):String = {
	  println(s"udd info => ${udd}")
	  udd
	}
	
/**
 * Funzione iterativa che elimina i file nella cartella temporanea.
 * @param f path del file da cancellare, se è una directory individua i file xml in essa contenuti, se è un file concella.
 */
	def delete(f:File):Boolean = {
    if (f.isDirectory()) {
      for (c <- f.listFiles())
        delete(c);
    }
		//println(f.getAbsolutePath)
    if (!f.delete()){
      false
    }else{
      true
    }
   
}
	
	def scansionaTmpDirPath(tmpDirPath:String, slash:String) : List[String] = {
	  try{
	    
  	  val xmls = new File ( tmpDirPath )
  		val xmlPathList = if(xmls.exists() && xmls.isDirectory() && !xmls.listFiles().isEmpty){
	    val xmlList = xmls.listFiles().map{ xml => 
	         val path = "file://" + slash +  xml.getPath
 		       path
	       }
	       xmlList.toList
	     }else{
	       List( )
	     }
			xmlPathList
		} catch {
				case e: Exception => {
					e.printStackTrace()
					throw new Exception(e)
				}
	  }
	}
	
	def ottieniAlberatura(file:String, rootDir:String, injectionTmp:String) : String = {
	  val dirPath = file.substring(file.lastIndexOf(rootDir), file.lastIndexOf(File.separator))
	  val tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)
	  tmpDirPath
	}
	
/**
 * Decomprime i file compressi nella cartella temporanea.
 * @param file nome del file compresso da decomprimere.
 * @param rootDir cartella root che contiene i file compressi.
 * @param injectionTmp cartella temporanea di destinazione dei file di misura decompressi.
 */
	def decomprimiAlberatura(file:String, rootDir:String, injectionTmp:String,printFiles:Boolean,listfiles:Broadcast[ListBuffer[Row]],dataelaborazione:java.sql.Timestamp,annomese_files:Int) : Unit = {

    var tmpDirPath=""
    //var ff=listfiles.value
		try {
			val zipArchive = new ZipArchive()
      zipArchive.timeStampDecompressione(dataelaborazione)
      zipArchive.annomeseDecompressione(annomese_files)

			val dirPath = file.substring(file.lastIndexOf(rootDir), file.lastIndexOf(File.separator))
			 tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)

			val tmpDir = new File(tmpDirPath)

			if (new File(file).canRead) {
				tmpDir.mkdirs()
				//zipArchive.unZipIt(file,tmpDirPath)
				zipArchive.unZip(file, tmpDirPath,false,printFiles)
        this.synchronized {
          listfiles.value ++= zipArchive.listFilesBuff
        }
				//tmpDirPath
			}
      else {
        val sp = file.split(File.separator)
        val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
				this.synchronized {
					listfiles.value += Row("003", s"Il file ${file} non è stato decompresso poichè non è possibile leggerlo", file, tmpDirPath, annoMeseGiornoDir, "E", dataelaborazione, annomese_files)
				}
        log.info("*** Il file : " + file + " non è stato decompresso poichè non è possibile leggerlo")
      }

		} catch {
			case e: Exception => {
        val sp = file.split(File.separator)
        val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
				this.synchronized {
					listfiles.value += Row("004", s"Errore in decompressione file causa ${e.getMessage}", file, tmpDirPath, annoMeseGiornoDir, "E", dataelaborazione, annomese_files)
				}
        log.info("Errore in decompressione per il file " + file + " causa: " + e.getMessage)
			}
		}
	}


	/**
 * Scansiona gli archivi compressi nelle cartelle dei distributori in base all'anno, mese, giorno specificati.
 * @param uddDir cartella del distributore.
 * @param args argomenti passati da linea di comando al processo di decompressione.
 * @param commonsCliUtils oggetto di utilità per la gestione degli argomenti da linea di comando.
 * @param commandLineOptions oggetto di utilità per la gestione degli argomenti da linea di comando.
 * @return elenco archivi compressi.
 */
	def leggiAlbertatura(uddDir:String, giorniList:List[String], args: Array[String], commonsCliUtils:CommonsCliUtils, commandLineOptions:CommandLineOptions,listfiles:Broadcast[ListBuffer[Row]]=null,dataelaborazione:java.sql.Timestamp=null,annomese_files:Int=0) : List[String] = {
	  try{
	  val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = commonsCliUtils.getArgs(commandLine)
	  val isAggiorna:Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)
		val is_ammissibilita=commandLine.hasOption(commandLineOptions.decomprimeAmmissibilita.getOpt)



		val pdo_rfo:String = argsObjMaster.PdoRfo
		val isGiornoSingolo:Boolean = if(commandLine.hasOption(commandLineOptions.giorno.getOpt)){
			true
		}else{
			false
		}
	  val ret = giorniList.map{ giornoX =>

						/* singolo giorno */
						val argsObj = if(isGiornoSingolo){
							argsObjMaster
						}else{
							commonsCliUtils.getArgsRange(commandLine,commandLineOptions, giornoX)
							//commonsCliUtils.getArgs(commandLine,giornoIn= giornoX)
						}

						val anno:String = argsObj.anno
						val mese:String = argsObj.mese
						val giorno:String = argsObj.giorno
						val nomeFile:String = argsObj.nomeFile
						val nomeFile2G:String = argsObj.nomeFile2G
						//println(s"*** anno: ${anno} - mese: ${mese} - ${giorno} ")
						val xmlDirPath = uddDir + File.separator + anno + File.separator + mese + giorno
						//println("*** xmlDirPath: " + xmlDirPath)

						val annoMeseGiornoDir = anno + mese + giorno
						val xmlDir = new File( xmlDirPath )
						/* scansiona i file di ogni udd */
						val abW3 = if(xmlDir.exists() && xmlDir.isDirectory() && !xmlDir.listFiles().isEmpty){
							val xmlFiles = xmlDir.listFiles(new FilenameFilter {
								override def accept(dir: File, name: String): Boolean = {


										val name_upper=name.toUpperCase

    								/*val ret:Boolean = if(pdo_rfo.equals("RFO")) {
												(name_upper.contains("_RFO_") || name_upper.contains("_RFO2G_")) && (name.endsWith(".zip") || name.endsWith(".ZIP"))
										}else if(pdo_rfo.equals("ALTRI_FLUSSI")) {
												!( name_upper.contains("_RFO_") || name_upper.contains("_RFO2G_")  || name_upper.contains("_PDO_") || name_upper.contains("_PDO2G_") || name_upper.contains("_SMIS")) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
										}else if(pdo_rfo.equals("SMIS")) {
											name_upper.contains("_SMIS") && (name.endsWith(".zip") || name.endsWith(".ZIP"))
										}else{
											if(is_ammissibilita && argsObjMaster.tipoflusso_estrazione_notset)
												(name.endsWith(".zip") || name.endsWith(".ZIP") )
											else
    								     (name_upper.contains("_PDO_") || name_upper.contains("_PDO2G_") ) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
    								}*/

									val ret:Boolean = if(pdo_rfo.equals(TypeDataToElab.Rfo.toString)) {
										(name_upper.contains(s"${TypeDataToElab.Rfo.toString}") || name_upper.contains(s"${TypeDataToElab.Rfo.toString}2G")) && (name.endsWith(".zip") || name.endsWith(".ZIP"))
									}else if(pdo_rfo.equals(TypeDataToElab.Other_Data.toString)) {
										!( name_upper.contains(s"${TypeDataToElab.Rfo.toString}") ||
											 name_upper.contains(s"${TypeDataToElab.Rfo.toString}2G")  ||
											 name_upper.contains(s"${TypeDataToElab.Pdo.toString}") ||
											 name_upper.contains(s"${TypeDataToElab.Pdo.toString}2G") ||
											name_upper.contains(s"${TypeDataToElab.Smis.toString}")) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
									}else if(pdo_rfo.equals(TypeDataToElab.Smis.toString)) {
										name_upper.contains(s"${TypeDataToElab.Smis.toString}") && (name.endsWith(".zip") || name.endsWith(".ZIP"))
									}else{
										if(is_ammissibilita && argsObjMaster.tipoflusso_estrazione_notset)
											(name.endsWith(".zip") || name.endsWith(".ZIP") )
										else
											(name_upper.contains(s"${TypeDataToElab.Pdo.toString}") || name_upper.contains(s"${TypeDataToElab.Pdo.toString}2G") ) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
									}

									//nel caso di switch i distributori possono mandare misure future e quindi bisogna evitare il controllo sulla data della misura


									  val isSwitching=if(name_upper.contains("SMIS") || name_upper.contains("SNS") || name_upper.contains("SOS") || name_upper.contains("SNF") || name_upper.contains("SOF") ) true else false
 									  val otmp = name.split("_")

    								val rr = if(ret)  {
    								  val containsName2G = name.contains("_2G")
											val tmp =if(otmp.length > 2) otmp(2) else ""


    								  val nameSplit = Try(tmp.toInt).getOrElse(nomeFile.toInt)
											//commento : nomeFileInt=annomese
											//commento : nomeFile2GInt=annomesegiorno
    								  val nomeFile2GInt = nomeFile2G.toInt
    								  val nomeFileInt = nomeFile.toInt
    								  val r = if(containsName2G){
												val nameSplit2 = (if(tmp.length==6)tmp+"01" else nameSplit.toString).toInt
    								    val a = if(isSwitching || nameSplit2 <= nomeFile2GInt) {
    								      true
    								      }else {
                          if(listfiles!=null) {

                            this.synchronized {
                              val strfilename = xmlDir + File.separator + name
                              listfiles.value += Row("005", s"Il file ${name} non verrà decompresso a causa del nome file avente data : ${nameSplit2} > del  mese corrente", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
                            }

                            log.info(s"Il file ${name} non verrà decompresso a causa del nome file avente data : ${nameSplit2} > del  mese corrente")
                          }
                          false
                        }
    								    a
    								  }else{
    								    val b = if(isSwitching || nameSplit <= nomeFileInt){
    								      true
    								      } else {
                          if(listfiles!=null) {

                            this.synchronized {
                              val strfilename = xmlDir + File.separator + name
                              listfiles.value += Row("005", s"Il file ${name} non verrà decompresso a causa del nome file avente data : ${nameSplit} > del  mese corrente", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
                            }

                            log.info(s"Il file ${name} non verrà decompresso a causa del nome file avente data : ${nameSplit} > del  mese corrente")
                          }
                          false
                        }
    								    b
    								  }
    								  r
    								}else {

											val name_upper_case=name.toUpperCase
											val has_flusso=tipi_di_flusso.exists(name_upper_case.contains(_))

											if((!has_flusso) && ( name_upper_case.endsWith(".ZIP")  ))
											{
												if (listfiles != null) {
													this.synchronized {
														val strfilename = xmlDir + File.separator + name
														listfiles.value += Row("006", s"Il file ${name} non verrà elaborato poichè il nome file non ha un tipo flusso che ricade tra i flussi previsti : ${tipi_di_flusso.toString}", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
													}
												}

											}
											false
    								}
    								rr
								}
							})

							val ret = xmlFiles.toList
							ret
						} else {
						  List( )
						}
						abW3
	    }
  	  val retList = ret.flatMap(f => f).map(_.getPath)
  	  retList
  	  	 } catch {
  				case e: Exception => {
  					e.printStackTrace()
  					throw new Exception(e)
  				}
  	}
	}
	
/**
 * Scansiona le cartelle dei distributori.
 * @param injectionPath path delle cartelle dei distributori.
 * @return elenco archivi compressi.
 */
		def scansionaUDD(injectionPath:String) : List[String] = {
			val abW2 = new ArrayBuffer[(String)]()

					//cartella principale
					for (xmlPrincipaleDir <- new File(injectionPath).listFiles()) {
										//println("XXX xmlPrincipaleDir: " + xmlPrincipaleDir.getPath)
						if(xmlPrincipaleDir.exists() && xmlPrincipaleDir.isDirectory() && !xmlPrincipaleDir.listFiles().isEmpty){
							//cartella del distributore
							val pivaDistrDir = xmlPrincipaleDir.getPath()

							try{
									for (xmlUddDir <- new File(pivaDistrDir + File.separator + "DISTRIBUTORE").listFiles()) {
										if( xmlUddDir.exists() && xmlUddDir.isDirectory() &&  !xmlUddDir.listFiles().isEmpty){
											//cartella del sotteso
											val uddDir = xmlUddDir.getPath()
											//println(uddDir)
											abW2.+=(uddDir)
										}
									}

							}catch{
          		case e: Exception =>  {
          		  e.printStackTrace()
          		  throw new Exception(e)
          		}
          	}

						}
					}
			abW2.toList
	}


	//in caso di aggiornamento considero solo i distributori_utenti specificati se ci sono
	def distrUteList(commandLine: CommandLine,commandLineOptions:CommandLineOptions, nomeFile:String) : Boolean = {
			if(commandLine.hasOption(commandLineOptions.distrUteAgg.getOpt)){
				val dul = commandLine.getOptionValue(commandLineOptions.distrUteAgg.getOpt).split(',')
						var ret:Boolean = false

						dul.foreach{f =>
						if(nomeFile.contains(f)){
							ret = true
						}
				}
				ret
			}else{
				true
			}
	}

  def writeDataElabEE_HDFS(remove:Boolean,dataelab:java.sql.Timestamp):Boolean = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    try {
      val filehdfs = new Path("/tmp/decomprime_ee")

      if (remove) {
        if (hdfs.exists(filehdfs))
          hdfs.delete(filehdfs, true)

      } else {

        if (hdfs.exists(filehdfs))
          hdfs.delete(filehdfs, true)


        val output = hdfs.create(filehdfs)
        val writer = new PrintWriter(output)

        try { writer.write(dataelab.toString) }
        catch { case x : Exception => return false }
        finally { writer.close() }

      }

      return true
    } catch {
      case e: Exception => { return false }


    }
  }


}
