package it.au.misure.ingestione

import java.io.{File, FilenameFilter}
import java.lang.management.ManagementFactory

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.util.{CreateProperties, LoggingSupport, ZipArchive}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Try
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import it.au.misure.util.Schemas._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


/**
 * ==Flusso Misure Decompressione Gas==
 * Decomprime gli archivi 1G/2G su una tabella temporanea. I flussi misura vengo acquisiti in formato zip sotto un’alberatura di sottocartelle predefinita, 
 * quindi la prima fase è la lettura di tutti i file dell’alberatura che devono essere acquisiti.
 * Nella seconda fase i file vengono decompressi in una cartella temporanea mantenendo la stessa alberatura originale per essere successivamente acquisiti dal processo stesso.
 * 
 * 
 */
object Decomprime_Gas extends LoggingSupport {

	var rtv_val=false
  val tbl_elenco_decompressione="cmg_gas.flussi_da_recuperare"


	var sqlcontx:SQLContext=null
	val tipi_di_flusso = List("TFC","VPG","SW1","FUI","TDS","DEF","RGL","RML","TAL","TAV","TGL","RSL","RMV","TML","TMV","IM1","TAS","D01","A01","A40","SM1")

	def can_go_decompressione(da_recupero_puntuale:Boolean): Boolean ={
		val vmName = ManagementFactory.getRuntimeMXBean.getName
		val p = vmName.indexOf("@")
		val cur_pid = vmName.substring(0, p)

		val parAmm:String=if(da_recupero_puntuale)" -R" else ""
		//verifica che non ci sia decompressione o ingestione in corso
		val tmpDec = Try("ps aux" #| "grep it.au.misure.cli.FlussoMisureTool" #| s"grep -v ${cur_pid}" #| s"grep DGAS${parAmm}" !!) getOrElse("")
    val tmpInj = Try("ps aux" #| "grep ingestion_data" #| s"grep -v ${cur_pid}" #| s"grep .sh" !!) getOrElse("")

		var pidfound=""

		if(tmpDec!="" && tmpDec.contains(s".jar -DGAS${parAmm}"))
		{
			val vals=tmpDec.split(" ")
			val utente=vals(0)
			for(i<-1 to vals.length-1) {
				if (vals(i).trim() != "" && pidfound ==""){
					pidfound=vals(i)
					if(cur_pid!=pidfound){
						val descr = if(da_recupero_puntuale)"decompressione Gas per recupero puntuale" else "decompressione Gas"

						log.info(s"Attenzione è stato trovato un processo di ${descr} in corso , attendere la fine del processo in corso e riprovare")
						log.info(s"Utente processo ${descr} in corso : " + utente)
						log.info(s"PID processo ${descr} in corso : " + pidfound)
						return false
					}

				}
			}

		}

    pidfound=""
    if(tmpInj!="")
    {
      val vals=tmpInj.split(" ")
      val utente=vals(0)
      for(i<-1 to vals.length-1) {
        if (vals(i).trim() != "" && pidfound ==""){
           pidfound=vals(i)
          if(cur_pid!=pidfound) {
            val descr =  "ingestione"

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


  def decompressioneDaElencoFiles(): Unit ={

  }
	def main(args: Array[String]) {


		val commonsCliUtils = new CommonsCliUtils()
		val commandLineOptions = new CommandLineOptions()
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = commonsCliUtils.getArgs_Gas(commandLine)


    val tipo_flusso_tmp: String = argsObjMaster.PdoRfo
		//log.info(s"*** ${tipo_flusso_tmp}")
    val verbose:Boolean = if(tipo_flusso_tmp.contains("-V"))true else false
		val daelenco:Boolean=if(tipo_flusso_tmp.contains("-R"))true else false
    val test:Boolean = if(tipo_flusso_tmp.contains("-T") || tipo_flusso_tmp.contains("-K"))true else false
		val dataelaborazione = new java.sql.Timestamp(System.currentTimeMillis())


    println("*** anno: " + argsObjMaster.anno.toString)
    println("*** mese: " + argsObjMaster.mese.toString)
    //if(daelenco)
    //println("*** Decompressione da recupero puntuale ***")

    val tipo_flusso: String = tipo_flusso_tmp.replace("-V", "").replace("-T", "").replace("-K", "").replace("-R", "")

    if(daelenco && (commandLine.hasOption(commandLineOptions.giorno.getOpt) || commandLine.hasOption(commandLineOptions.giorni.getOpt)))
     {
       log.info("*** Attenzione nella procedura di decompressione da elenco files non bisogna specificare il giorno/giorni ***")
       return
     }

    val isGiornoSingolo: Boolean = if (commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
        println("*** giorno: " + argsObjMaster.giorno)
        true
      } else {
        false
      }

      val giorniList: List[String] = if (isGiornoSingolo) {
        List("giorno_singolo")
      } else {
        if(daelenco)
          List()
        else
         commonsCliUtils.getGiorni(commandLine, commandLineOptions)
      }

      if (!daelenco && !isGiornoSingolo)
        println("*** giorni: " + giorniList.toString())


      if (!can_go_decompressione(daelenco)) return
    if(!daelenco)
      println(s"***** Inizio processo Decompressione Misure GAS - ${tipo_flusso}*****")
    else
      println(s"***** Inizio processo Decompressione Misure GAS da elenco files *****")

		val nameApp=argsObjMaster.appName

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

    sqlcontx = new HiveContext(sc)
    sqlcontx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sqlcontx.setConf("spark.sql.parquet.compression.codec", "snappy")
    sqlcontx.setConf("spark.sql.parquet.binaryAsString", "true")
    sqlcontx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
    sqlcontx.setConf("hive.exec.dynamic.partition", "true")
    sqlcontx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    sqlcontx.setConf("spark.sql.parquet.mergeSchema", "false")
    sqlcontx.setConf("spark.sql.parquet.filterPushdown", "true")
    sqlcontx.setConf("spark.sql.hive.metastorePartitionPruning", "true")
    //sqlCtx.setConf("spark.sql.hive.convertMetastoreParquet", "false")

		try {

      val slash = sc.getConf.get("spark.flusso.misure.slash")
      val minPartitions = sc.getConf.get("spark.flusso.misure.min.partitions").toInt
      val injectionTmp: String = argsObjMaster.injectionTmp
      val rootDir: String = argsObjMaster.rootDir


      //val injection: String = if(daelenco && test) s"/mnt/isilonshare1/GAS_INJ/gas_test/isilonshare_gas/" else s"/mnt/${rootDir}/"

			val injection: String =  s"/mnt/${rootDir}/"

			val report_decompressione = sc.getConf.get("spark.flusso.misure.gas.reportdecompressione")  + (if(test) "_test" else "")

      //println("*** sc.master: " + sc.master)
      println("*** sc.sparkUser: " + sc.sparkUser)
      println("*** injection: " + injection)
      println("*** injectionTmp: " + injectionTmp)
      println("*** rootDir: " + rootDir)
      println("*** minPartitions: " + minPartitions)

      if(daelenco && tipo_flusso=="")
        println(s"*** tipo di flussi trovati nella tabella ${tbl_elenco_decompressione}")
      else if(tipo_flusso=="")
      println(s"*** tipo di flusso: tutti tra quelli previsti")
      else
        println(s"*** tipo di flusso: ${tipo_flusso}")

      println(s"*** stampa elenco file decompressi : ${verbose}")

      println("*** percorso report decompressione: " + report_decompressione)

      if (report_decompressione == "") {
        log.info("ERRORE: bisogna impostare il percorso di scrittura relativo al report di decompressione")
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
      println("*** controllo della cartella temporanea OK")

      println("*** Avvio Scansione alberatura per UDD")

      val elencoUDD = scansionaUDD(injection,daelenco,tipo_flusso).toSeq

      val rddUDD = sc.parallelize(elencoUDD).setName("Scansiona alberatura")
      rddUDD.cache()
      println("*** scansiona alberatura UDD OK ")


     val numfdec: org.apache.spark.Accumulator[Int] = sqlcontx.sparkContext.accumulator(0, "Files_decompressi")
      var listFiles = ListBuffer[Row]()
      var zF = sqlcontx.sparkContext.broadcast(listFiles)

      //if (modalitaMaster.equals("local[*]")) {

      var numthread = 200


      val annomese_files = (argsObjMaster.anno + argsObjMaster.mese).toInt


      val rowsFiles = if(daelenco)
        {
          val where=if(tipo_flusso!="")s" and tipo_flusso = '${tipo_flusso}' " else ""

          log.info(s"Avvio estrazione di tutti i files da recuperare dalla tabella ${tbl_elenco_decompressione}")
          val ff=sqlcontx.sql(
        s"""
         select annodir,LPAD(mesedir,2,0)mesedir,LPAD(giornodir,2,0)giornodir,Filename,nvl(ArchivioZip,'')ArchivioZip,pathsrc,NFile[0] Distr , NFile[1] Udd
          from
          (
            select annodir,mesedir,giornodir,ArchivioZip,
            nvl(Nomefile,'')  Filename,
            case nvl(Nomefile,'') when '' then split(ArchivioZip,'_') else split(Nomefile,'_') end NFile,
            nvl(pathsrc,'')pathsrc
            from ${tbl_elenco_decompressione}
            where not(nvl(Nomefile,'') = '' and nvl(ArchivioZip,'') = '') ${where}
           ) as tbl  order by annodir,mesedir,giornodir
        """.stripMargin).collect()

        log.info("Tot files da elaborare presenti in tabella : " + ff.length.toString )
          ff}else null

      rddUDD.foreach { uddDir =>
        // println(s"${uddDir}")
        // println(s"${giorniList}")

				var list = new ArrayBuffer[Thread]()

        val ret =
            if(!daelenco)leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions, zF, dataelaborazione, annomese_files)
            else leggiAlbertatura_DaElenco(rowsFiles,uddDir,zF,dataelaborazione,annomese_files)

        ret.foreach(filePath => {
          val t = new Thread {

            override def run {
              decomprimiAlberatura(filePath._1, filePath._2, rootDir, injectionTmp, verbose, zF, dataelaborazione, annomese_files)

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

      if(daelenco)
        {
          log.info("*** Avvio verifica path UDD in recupero puntuale")

          for (r <- rowsFiles) {

            val distr="TMG_" + r.getAs[String]("Distr")
            val udd=distr+"_"+r.getAs[String]("Udd")

            val anno: String = r.getAs[String]("annodir")
            val mese: String = r.getAs[String]("mesedir")
            val giorno: String = r.getAs[String]("giornodir")
            val filetoTryDecomprime = r.getAs[String]("ArchivioZip")
            val filexml = r.getAs[String]("Filename")
            val pathsrc = r.getAs[String]("pathsrc")

            val xmlDirPath = if(pathsrc=="")new File(injection + File.separator + distr + File.separator + "DISTRIBUTORE"+ File.separator + udd + File.separator + anno + File.separator + mese + giorno) else new File(pathsrc)
            if(!xmlDirPath.exists())
            {
              val fname = if(filetoTryDecomprime!="") filetoTryDecomprime else  filexml
              log.info(s"ATTENZIONE il path ${xmlDirPath} relativo al file ${fname} non esiste verificare i dati immessi ")
            }

          }
        }

      log.info("*** Avvio processo verifica file xml decompressi")

      val tmpDirPath = injectionTmp.concat(File.separator).concat(rootDir)
      val totF=getCountFiles(new File(tmpDirPath))
      log.info(s"*** Tot files scritti nel path ${tmpDirPath} :" + totF.toString +" ****")





      /*rddUDD.foreach { uddDir =>
        val ret = if(!daelenco)leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions)
                  else leggiAlbertatura_DaElenco(rowsFiles,uddDir)

        var dictfiles_udd: Map[String, String] = Map("" -> "")
        ret.foreach(filePath => {
          val dirPath = filePath._1.substring(filePath._1.lastIndexOf(rootDir), filePath._1.lastIndexOf(File.separator))
          val tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)

          if (!dictfiles_udd.contains(tmpDirPath)) {

            val listFF = new File(tmpDirPath).listFiles
						if(listFF!=null)
            numfdec += listFF.length

            dictfiles_udd += (tmpDirPath -> "ok")
          }

        })
        dictfiles_udd.empty
        dictfiles_udd = null
      }


      log.info("*** Numero di file decompressi : " + numfdec.value.toString)

      if(totF!=numfdec.value)
        log.info(s"*** ATTENZIONE SI E' VERIFICATA UN'INCONGUENZA TRA IL NUMERO DI FILES SCRITTI ED IL NUMERO DI FILES ELABORATI IN BASE AI PARAMETRI IN INGRESSO : ${totF} <> ${numfdec.value}. VERIFICARE IL PRIMA POSSIBILE")
   */

      if (listFiles.length != totF) {
        log.info(s"*** ATTENZIONE SI E' VERIFICATA UN'INCONGUENZA TRA I FILE DECOMPRESSI E L'ELECO DEI FILE DA SCRIVERE SU HIVE : ${totF} <> ${listFiles.length}")

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

            if((!has_flusso) && ( name_upper_case.endsWith(".ZIP") || name_upper_case.endsWith(".XML") ))
              log.info(s"Il file ${f2.getName} non verrà elaborato poichè il nome file non ha un tipo flusso che ricade tra i flussi previsti : ${tipi_di_flusso.toString}")
            else
              log.info("File sorgente :" + el.getString(2) + " non decompresso /copiato")
          }
				}
				if(counter==0)
					log.info(s"*** ATTENZIONE L'INCONGRUENZA SULLA NUMEROSITA E' DOVUTA A FILE SORGENTI DUPLICATI - VERIFICARE LA TABELLA report_decompressione SU HIVE")

			}
      val rddToWrite = sc.parallelize(listFiles.toList)

      val dfQS3 = sqlcontx.createDataFrame(rddToWrite, schemaReportDecompressione)

      log.info("***** creazione DataFrame Report misure gas decompresse OK")

      dfQS3
        .write.option("parquet.block.size", blocksize.toString)
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy("annomese")
        .save(report_decompressione)


      sqlcontx.sql("MSCK REPAIR TABLE cmg_gas.report_decompressione"+ (if(test) "_test" else ""))


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
	
	def scansionaTmpDirPath(tmpDirPath:String, slash:String) : List[String] = {
	  try{
	    
  	  val xmls = new File ( tmpDirPath )
  		val xmlPathList = if(xmls.exists() && xmls.isDirectory() && !xmls.listFiles().isEmpty ){
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
 * @param fileIntoZip se file è un archivio e fileIntoZip è specificato verrà decompresso solamente il file indicato
 * @param rootDir cartella root che contiene i file compressi.
 * @param injectionTmp cartella temporanea di destinazione dei file di misura decompressi.
 */
	def decomprimiAlberatura(file:String,fileIntoZip:String, rootDir:String, injectionTmp:String,printFiles:Boolean,listfiles:Broadcast[ListBuffer[Row]],dataelaborazione:java.sql.Timestamp,annomese_files:Int) : Unit = {



		var tmpDirPath=""
		try {
      //log.info(s"Decompressione file : ${file} ")
			val zipArchive = new ZipArchive()
      zipArchive.timeStampDecompressione(dataelaborazione)
      zipArchive.annomeseDecompressione(annomese_files)

			val dirPath = file.substring(file.lastIndexOf(rootDir), file.lastIndexOf(File.separator))
			 tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)

			val tmpDir = new File(tmpDirPath)


			if (new File(file).canRead || new File(file).getName=="*") {
				tmpDir.mkdirs()
				zipArchive.unZip(file, tmpDirPath,true,printFiles,fileIntoZip)
				this.synchronized {
					listfiles.value ++= zipArchive.listFilesBuff
				}
			}
			else {
				val sp = file.split(File.separator)
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
				this.synchronized {
					listfiles.value += Row("003", s"Il file ${file} non è stato decompresso poichè non è possibile leggerlo", file, tmpDirPath, annoMeseGiornoDir, "U", dataelaborazione, annomese_files)
				}

				log.info("*** Attenzione il file : " + file + " non è stato decompresso poichè non è possibile leggerlo")
			}

		} catch {
			case e: Exception => {
				val sp = file.split(File.separator)
				val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
				this.synchronized {
					listfiles.value += Row("004", s"Errore in decompressione file causa ${e.getMessage}", file, tmpDirPath, annoMeseGiornoDir, "U", dataelaborazione, annomese_files)
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
	def leggiAlbertatura(uddDir:String, giorniList:List[String], args: Array[String], commonsCliUtils:CommonsCliUtils, commandLineOptions:CommandLineOptions,listfiles:Broadcast[ListBuffer[Row]]=null,dataelaborazione:java.sql.Timestamp=null,annomese_files:Int=0) : List[(String,String)] = {
	  var xmlDirPath=""
		try{
	  val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = commonsCliUtils.getArgs_Gas(commandLine)


		val tipo_flusso:String = argsObjMaster.PdoRfo.replace("-V","").replace("-T","").replace("-R","")
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
			      //per il gas non dovrebbe esserci il 2G
						val nomeFile2G:String = argsObj.nomeFile2G
						//println(s"*** anno: ${anno} - mese: ${mese} - ${giorno} ")
						 xmlDirPath = uddDir + File.separator + anno + File.separator + mese + giorno
						//println("*** xmlDirPath: " + xmlDirPath)

						val annoMeseGiornoDir = anno + mese + giorno

						val xmlDir = new File( xmlDirPath )
						/* scansiona i file di ogni udd */
						val abW3 = if(xmlDir.exists() && xmlDir.isDirectory() && !xmlDir.listFiles().isEmpty){
							val xmlFiles = xmlDir.listFiles(new FilenameFilter {
								override def accept(dir: File, name: String): Boolean = {

    								val ret:Boolean = if(argsObjMaster.tipoflusso_estrazione_notset)
												(name.toLowerCase.endsWith(".zip") || name.toLowerCase.endsWith(".xml") )
											else
    								     (name.toUpperCase.contains(s"${tipo_flusso}")) && ( name.toLowerCase.endsWith(".zip") || name.toLowerCase.endsWith(".xml") )

									  //println("file name " + name)
									//nel caso di switch i distributori possono mandare misure future e quindi bisogna evitare il controllo sulla data della misura


									  val isSwitching=false //if(name.toUpperCase().contains("_SMIS") || name.toUpperCase().contains("_SNS") || name.toUpperCase().contains("_SOS") ) true else false
 									  val otmp = name.split("_")


    								val rr = if(ret)  {

    								  val containsName2G = false //name.contains("_2G")
											val tmp =if(otmp.length > 2) otmp(2) else ""


    								  //val nameSplit = Try(tmp.toInt).getOrElse(nomeFile.toInt)
											val nameSplit = nomeFile.toInt
											//commento : nomeFileInt=annomese
											//commento : nomeFile2GInt=annomesegiorno
    								  val nomeFile2GInt = nomeFile2G.toInt
    								  val nomeFileInt = nomeFile.toInt
											//PER IL MOMENTO IL CONTROLLO SUI 2G NON ESISTE SUL GAS
    								  val r = if(containsName2G){
												println(s"il file ${name} è un 2G")
												val nameSplit2 = (if(tmp.length==6)tmp+"01" else nameSplit.toString).toInt
    								    val a = if(isSwitching || nameSplit2 <= nomeFile2GInt) {
    								      true
    								      }else {
                          if(listfiles!=null) {
														this.synchronized {
															val strfilename = xmlDir + File.separator + name
															listfiles.value += Row("005", s"Attenzione il file ${name} non verrà elaborato a causa del nome file avente data : ${nameSplit2} > del mese corrente", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
														}
                          }
                          log.info(s"Attenzione il file ${name} non verrà elaborato a causa del nome file avente data : ${nameSplit2} > del mese corrente")
                          false
                        }
    								    a
    								  }else{
    								    val b = if(isSwitching || nameSplit <= nomeFileInt){
    								      true
    								      } else {
                          if (listfiles != null) {
														this.synchronized {
															val strfilename = xmlDir + File.separator + name
															listfiles.value += Row("005", s"Attenzione il file ${name} non verrà elaborato a causa del nome file avente data : ${nameSplit} > del mese corrente", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
														}
                          }
                          log.info(s"Attenzione il file ${name} non verrà elaborato a causa del nome file avente data : ${nameSplit} > del mese corrente")
                          false
                        }
    								    b
    								  }
    								  r
    								}else {

											val name_upper_case=name.toUpperCase

											if(( name_upper_case.endsWith(".ZIP") || name_upper_case.endsWith(".XML") ))
												{
                          val flusso_file_non_previsto= !(tipi_di_flusso.exists(name_upper_case.contains(_)))

                          if(flusso_file_non_previsto) {
                            if (listfiles != null) {
                              this.synchronized {
                                val strfilename = xmlDir + File.separator + name
                                if(argsObjMaster.tipoflusso_estrazione_notset)
                                  listfiles.value += Row("006", s"Attenzione il file ${name} non verrà elaborato poichè il nome file non contiene un tipo di flusso tra quelli previsti: ${tipi_di_flusso}", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
                                else
                                  listfiles.value += Row("006", s"Attenzione il file ${name} non verrà elaborato poichè il nome file non contiene un tipo di flusso tra quelli previsti: ${tipi_di_flusso} , il tipo di flusso specificato è : ${tipo_flusso}", strfilename, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
                              }
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
  	  val retList = ret.flatMap(f => f).map( el => (el.getPath,""))
  	  retList
  	  	 } catch {
  				case e: Exception => {
						println(s"*** PATH IN ERRORE ${xmlDirPath}")
  					e.printStackTrace()
						List()
  					//throw new Exception(e)
  				}
  	}
	}

  def leggiAlbertatura_DaElenco(rr:Array[Row],uddDir:String,listfiles:Broadcast[ListBuffer[Row]]=null,dataelaborazione:java.sql.Timestamp=null,annomese_files:Int=0) : List[(String,String)] = {
    var xmlDirPath=""
    try{


      val uddDirF=new File(uddDir)
      val data = uddDirF.getName().split("_")
      val udd=data.takeRight(1)(0)
      val distr=data.takeRight(2)(0)


      val rows =rr.filter(r => {
        if(r.getAs[String]("pathsrc")!="" )
					{
            val tmp = r.getAs[String]("pathsrc").split("/")

            val pathscr =if (tmp.length > 6) {
              val tmp2: Array[String] = new Array(6)
              tmp.copyToArray(tmp2, 0, 6)
              tmp2.mkString("/")
            }
            else
              r.getAs[String]("pathsrc")

						//log.info(s"*** Avvio ricerca per path ${pathscr} ***")
          (uddDirF.getPath==pathscr)
					}
        else {
					//log.info(s"*** Avvio ricerca per distributore ${distr} e udd ${udd} ***")
					(r.getAs[String]("Distr") == distr && r.getAs[String]("Udd") == udd)
				}
      })

      var lb = new ListBuffer[(String,String)]
      for (r <- rows) {
        val anno: String = r.getAs[String]("annodir")
        val mese: String = r.getAs[String]("mesedir")
        val giorno: String = r.getAs[String]("giornodir")
        val filetoTryDecomprime = r.getAs[String]("ArchivioZip")
        val filexml = r.getAs[String]("Filename")

        xmlDirPath = uddDir + File.separator + anno + File.separator + mese + giorno

        //log.info(s"Ricerca nel path : ${xmlDirPath}")

        val annoMeseGiornoDir = anno + mese + giorno

        val xmlDir = new File(xmlDirPath)
				val filename_full = if(filetoTryDecomprime!="" && filetoTryDecomprime!="*")xmlDirPath + File.separator + filetoTryDecomprime else if(filetoTryDecomprime=="*")xmlDirPath else xmlDirPath + File.separator + filexml

        if (xmlDir.exists() && xmlDir.isDirectory() && !xmlDir.listFiles().isEmpty) {

          val filenamefull_F = new File(filename_full)

					//if()
          //log.info(s"Verifica esistenza file : ${filename_full}")

          if (filenamefull_F.exists() && !filenamefull_F.isDirectory()) {
            lb.append((filenamefull_F.getPath,filexml))
          }
          else if (filenamefull_F.exists() && filenamefull_F.isDirectory() && filetoTryDecomprime=="*") {
            lb.append((filenamefull_F.getPath+File.separator+"*",filexml))
          }
          else {
            if (listfiles != null) {
              this.synchronized {

                listfiles.value += Row("007", s"Attenzione il file ${filenamefull_F.getName} non verrà elaborato poichè non è stato trovato nella directory sorgente : ${xmlDirPath}", filename_full, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
              }
            }

            log.info(s"Attenzione il file ${filenamefull_F.getName} non verrà elaborato poichè non è stato trovato nella directory sorgente : ${xmlDirPath}")

          }
        }else
					{
						if (listfiles != null) {
							this.synchronized {

								listfiles.value += Row("007", s"Attenzione il path ${xmlDir.getPath} relativo al file ${filename_full} è inesistente", filename_full, xmlDirPath, annoMeseGiornoDir.toInt, "E", dataelaborazione, annomese_files)
							}
						}
						log.info(s"Attenzione il path ${xmlDir.getPath} relativo al file ${filename_full} è inesistente ")
					}
      }

        return lb.toList


    } catch {
      case e: Exception => {
        println(s"*** PATH IN ERRORE ${xmlDirPath}")
        e.printStackTrace()
        List()
        //throw new Exception(e)
      }
    }
  }
	
/**
 * Scansiona le cartelle dei distributori.
 * @param injectionPath path delle cartelle dei distributori.
 * @return elenco archivi compressi.
 */
		def scansionaUDD(injectionPath:String,daelenco:Boolean,tipo_flusso:String="") : List[String] = {

      var dictDistrUdd:Map[String,ListBuffer[String]] = Map("" -> new ListBuffer())

      if(daelenco)
        {
					val where=if(tipo_flusso!="")s" and tipo_flusso = '${tipo_flusso}' " else ""

          val rows:Array[Row] = sqlcontx.sql(
          s"""
          select distinct NFile[0]Distributore , NFile[1] UDD,pathsrc
          from
          (
            select
          case nvl(Nomefile,'') when '' then split(ArchivioZip,'_') else split(Nomefile,'_') end NFile,
          nvl(pathsrc,'')pathsrc
          from ${tbl_elenco_decompressione}
          where not(nvl(Nomefile,'') = '' and nvl(ArchivioZip,'') = '') ${where}
          ) as tbl """).collect()



          for (r <- rows){
           val distr= r.getString(0)
           val udd= r.getString(1)
           val pathscr= r.getString(2)

            if(pathscr=="") {
              if (!dictDistrUdd.contains(distr)) {
                val l = new ListBuffer[String]()
                l.append(udd)
                dictDistrUdd += (distr -> l)
              }
              else {
                val l = dictDistrUdd(distr)
                l.append(udd)
              }
            }else
              {
                val tmp = pathscr.split("/")

                val pathscr_x =if (tmp.length > 6) {
                  val tmp2: Array[String] = new Array(6)
                  tmp.copyToArray(tmp2, 0, 6)
                   tmp2.mkString("/")
                }
                else if (tmp.length < 6) {
                  log.info(s"*** Attenzione il path src ${pathscr}  non verrà inserito nell'elenco dei path degli UDD poichè non rispetta il formato previsto!")
                 ""
                }
                else
                  pathscr

                if (pathscr_x!="" && !dictDistrUdd.contains(pathscr_x)) {
                    dictDistrUdd += (pathscr_x -> ListBuffer(pathscr_x))
                }
              }

          }
        }
			val abW2 = new ArrayBuffer[(String)]()

					//cartella principale
					for (xmlPrincipaleDir <- new File(injectionPath).listFiles()) {
										//println("XXX xmlPrincipaleDir: " + xmlPrincipaleDir.getPath)
						if(xmlPrincipaleDir.exists() && xmlPrincipaleDir.isDirectory() && !xmlPrincipaleDir.listFiles().isEmpty){
              var canGo=true
							//cartella del distributore
              if(daelenco)
                {
                  val distr=xmlPrincipaleDir.getName.split("_").takeRight(1)(0)

                  if(!dictDistrUdd.contains(distr))
                     canGo=false

                  //else
                   // log.info(s"**** Distributore  ${distr} trovato nell'elenco da elaborare")
                }

                try {
                  if (canGo) {

                    val pivaDistrDir = xmlPrincipaleDir.getPath()
                    val distr = xmlPrincipaleDir.getName.split("_").takeRight(1)(0)

                    for (xmlUddDir <- new File(pivaDistrDir + File.separator + "DISTRIBUTORE").listFiles()) {
                      if (xmlUddDir.exists() && xmlUddDir.isDirectory() && !xmlUddDir.listFiles().isEmpty) {
                        //cartella del sotteso
                        if (daelenco) {
                          val l = dictDistrUdd(distr)
                          val udd = xmlUddDir.getName.split("_").takeRight(1)(0)
                          if (l.filter(el => {
                            el == udd
                          }).length != 0) {
                            //log.info(s"**** Trovato Udd :${udd} da elaborare")
                            val uddDir = xmlUddDir.getPath()
                            abW2.+=(uddDir)
                          }
                        }
                        else {
                          val uddDir = xmlUddDir.getPath()
                          //println(uddDir)
                          abW2.+=(uddDir)
                        }
                      }
                    }
                  }

                  //verifico nel caso di recupero da elenco tutti i record aventi pathsrc popolato
                  //e lo inserisco nell'elenco degli udd da recuperare
                  if (daelenco) {
                    for (elk <- dictDistrUdd.keys) {
                      {
                        val l: ListBuffer[String] = dictDistrUdd(elk)
                        if (elk != "" && l.length == 1 && l(0) == elk) {
                          if (!abW2.contains(elk)) {
															log.info(s"*** Inserimento path src ${elk} nell'elenco dei path degli UDD")
															abW2.+=(elk)
													}
                        }
                      }
                    }
                  }

                } catch {
                  case e: Exception => {
                    e.printStackTrace()
                    throw new Exception(e)
                  }
                }


						}
					}
			abW2.toList
	}

  /*
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
  }*/


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

  case class ParamsD(anno:String,mese:String,giorniList:List[String], args:Array[String],
                     commonsCliUtils:CommonsCliUtils, commandLineOptions:CommandLineOptions,
                     rootDir:String, injectionTmp:String, verbose:Boolean,
                     dataelaborazione:java.sql.Timestamp)

}
