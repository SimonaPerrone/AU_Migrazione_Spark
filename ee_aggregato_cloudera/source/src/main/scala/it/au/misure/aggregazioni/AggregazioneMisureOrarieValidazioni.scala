package it.au.misure.aggregazioni

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.io.File

import it.au.misure.util.Schemas._
import java.util.Properties
import it.au.misure.util.CreateProperties
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
//import it.au.misure.cli.CommonsCliUtils.Args

import it.au.misure.util.LoggingSupport
import org.apache.spark.storage.StorageLevel
import com.sun.org.apache.xalan.internal.xsltc.compiler.ValueOf

/**
 * ==Flusso Misure Aggregazione Misure Orarie==
 * Acquisisce le misure divise in quarti d'ora precedentemente elaborate dal processo
 * di ingestione denominato 'Flusso Misure Inserimento Misure Quarti' e le aggrega per ora. Al termine dell'aggregazione, il risultato viene salvato
 * nella tabella hdfs denominata ''aggregazioni_misure_orarie''.
 */
object AggregazioneMisureOrarieValidazioni extends LoggingSupport {

      /*
       * inizializzazione lettura file di properties
       */
			val propertiesC =new CreateProperties(System.getProperty("user.dir"))
	    val prop:Properties = propertiesC.prop
			val queryProp:Properties = propertiesC.query

			val _dbDest:String = prop.getProperty("spark.app.dbdest")
			val _basePath:String =prop.getProperty("spark.app.basepath")
			/*
			 * connessione db
			 */
			val url:String = prop.getProperty("spark.app.url")
			val user:String = prop.getProperty("spark.app.user")
			val password:String = prop.getProperty("spark.app.password")
			val driver = prop.getProperty("spark.app.jdbc.driver")
			Class.forName(driver)
			
			/*
			 * Flag controlli
			 */
			val flaguddpodApp:Boolean = prop.getProperty("spark.app.controllo.flaguddpod").toBoolean
      val trattamentoApp:Boolean = prop.getProperty("spark.app.controllo.trattamento").toBoolean
      val statoApp:Boolean = prop.getProperty("spark.app.controllo.stato").toBoolean
      val validatoApp:Boolean = prop.getProperty("spark.app.controllo.validato").toBoolean
      val flagAreaApp:Boolean = prop.getProperty("spark.app.controllo.flagarea").toBoolean
      val distrAziendaApp:Boolean = prop.getProperty("spark.app.controllo.distr_azienda").toBoolean
      val estraiAreaApp:Boolean = prop.getProperty("spark.app.controllo.estrai_area").toBoolean

          
      /**
       * Connessione utilizzata per la validazione area
       */
      val conn1:Connection = DriverManager.getConnection(url, user, password)
			/**
			 * Connessione utilizzata per la validazione stato pod ed esistenza RCU_misure per Mercato libero.
			 */
			val conn2:Connection = DriverManager.getConnection(url, user, password)
			/**
			 * Connessione utilizzata per al validazione orarie pod-udd.
			 */
			val conn3:Connection = DriverManager.getConnection(url, user, password)

			/**
			 * Connessione utilizzata per la vista azienda distributore.
			 */
			val conn4:Connection = DriverManager.getConnection(url, user, password)
			
			/**
			 * Connessione utilizzata ottenere l'area dal pod.
			 */
			val conn5:Connection = DriverManager.getConnection(url, user, password)
			
			/**
			 * Query utilizzata per validare ogni singolo giorno del mese nel file candidato per il POD in esame.
			 */
			val queryPs1 = queryProp.getProperty("spark.query.queryPs1")
			val queryPs11 = queryProp.getProperty("spark.query.queryPs11")

			/**
			 * Query utilizzata per la validazione Flag stato pod e controlla l'esistenza RCU_misure per Mercato libero.
			 */
			val queryPs2 = queryProp.getProperty("spark.query.queryPs2")

			/**
			 * Query utilizzata per la validazione Flag POD - area.
			 */
			val queryPs4 = queryProp.getProperty("spark.query.queryPs4")
			
			/**
			 * Estrazione area da pod
			 */
			val queryPs3 = queryProp.getProperty("spark.query.queryPs3")
			
			/**
			 * Query utilizzata per ottenere gli identificativi del distributore e del distributore di riferimento sulla base dati AU.
			 */
			val queryDistrAzienda = queryProp.getProperty("spark.query.queryDistrAzienda")
			
			/**
			 * Query che ottiene i dati di aggregazione con il timestamp maggiore per ottenere le misure più recenti.
			 */
			val queryFlussoMisureQuartiTimeStampMax = queryProp.getProperty("spark.query.flusso_misure_quarti.time_stamp_max")


	    val perdita_380 = prop.getProperty("spark.app.perdita.tensione.380").toDouble
	    val perdita_220 = prop.getProperty("spark.app.perdita.tensione.220").toDouble
	    val perdita_150 = prop.getProperty("spark.app.perdita.tensione.150").toDouble
	    val perdita_1_35 = prop.getProperty("spark.app.perdita.tensione.1_35").toDouble
	    val perdita_1 = prop.getProperty("spark.app.perdita.tensione.1").toDouble
			val testwithout_validations=false
			/**
			 * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
			 * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
			 */
			def main(args: Array[String]) {

	      val commandLineOptions = new CommandLineOptions()
	      val commonsCliUtils = new CommonsCliUtils()
				val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
				val argsObj = commonsCliUtils.getArgsAggregati(commandLine)
						

						val conf = new SparkConf()
						.setAppName( argsObj.appName )
						.set("spark.shuffle.service.enabled", "false")
						.set("spark.dynamicAllocation.enabled", "false")
						.set("spark.io.compression.codec", "snappy")
						.set("spark.rdd.compress", "false")
						.setMaster( argsObj.master )

						val sc = new SparkContext(conf)
						sc.setLogLevel("ERROR")
						
						val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

						val sqlCtx = new SQLContext(sc)
						sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
						sqlCtx.setConf("spark.sql.parquet.compression.codec","uncompressed")
						sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
						sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")

						val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
						
						val annoAggr:String =  argsObj.anno
						val meseAggr:String =  argsObj.mese
						val ghigliottina:Int = argsObj.annomesegiornodir
						val quarti = sc.getConf.get("spark.flusso.misure.quarti")
						val orarie =  sc.getConf.get("spark.aggregazioni.misure.orarie")+(if(testwithout_validations)"_tmp" else "")
						val tMillis = System.currentTimeMillis()
						val dataelaborazione = new java.sql.Timestamp( tMillis )
				    val annomese = annoAggr + meseAggr
				    val uidElab = sdf.format(new java.util.Date()).toLong
				    log.info("***** Inizio processo " + argsObj.appName + " *****")

						log.info("***** current user " + System.getProperty("user.name") + "****")
						log.info(propertiesC.printEnvVar)

						log.info("*** anno: " + annoAggr)
						log.info("*** mese: " + meseAggr)
						log.info("*** annomese: " + annomese)
						log.info("*** ghigliottina: " + ghigliottina)
						log.info("*** quarti: " + quarti)
						log.info("*** orarie: " + orarie)
						log.info("*** dataelaborazione: " + dataelaborazione)
						log.info("*** versione_orarie: " + uidElab)
						log.info("*** minPartitions: " + minPartitions)
						
						log.info("*** queryPs1: " + queryPs1)
						log.info("*** queryPs11: " + queryPs11)
						log.info("*** queryPs2: " + queryPs2)
						log.info("*** queryPs3: " + queryPs3)
						log.info("*** queryPs4: " + queryPs4)
						log.info("*** queryDistrAzienda: " + queryDistrAzienda)
						
						log.info(s"*** flaguddpod: ${flaguddpodApp}")
						log.info(s"*** trattamento: ${trattamentoApp}")
						log.info(s"*** stato: ${statoApp}")
						log.info(s"*** validato: ${validatoApp}")
						log.info(s"*** flagarea: ${flagAreaApp}")
						log.info(s"*** distr_azienda: ${distrAziendaApp}")
						log.info(s"*** estrai_area: ${estraiAreaApp}")

						val db = _dbDest

						log.info("*** database di destinazione : "+ db)
						
						/*
						 * creazione viste stato pod - trattamento
						 */
						creazioneVistaStatoPod(annomese)



					val queryQuarti:String = if(commandLine.hasOption(commandLineOptions.distrAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.distrAgg.getOpt).split(',').toList
					  s" and pivadistributorequarti in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else  if(commandLine.hasOption(commandLineOptions.uteAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.uteAgg.getOpt).split(',').toList
					  s" and pivautentequarti in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else if(commandLine.hasOption(commandLineOptions.noDistrAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.noDistrAgg.getOpt).split(',').toList
					  s" and pivadistributorequarti not in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else  if(commandLine.hasOption(commandLineOptions.noUteAgg.getOpt)){
					  val dul = commandLine.getOptionValue(commandLineOptions.noUteAgg.getOpt).split(',').toList
					  s" and pivautentequarti not in (${dul.map ( x => "'" + x + "'").mkString(",") })"
					  
					}else {
					  ""
					} 
	      
	      
	        val filesEsclusi = prop.getProperty("spark.app.aggregazioni.file.esclusi")
    			log.info(s"*** fileEsclusi: ${filesEsclusi}")
					val filesEsclusiRdd = sc.textFile(filesEsclusi).collect().toList
					
					val filesEsclusiQuery = if(filesEsclusiRdd.size > 0){
					  s" and nomefile not in (${filesEsclusiRdd.map ( x => "'" + x + "'").mkString(",") })"
					}else{
					  ""
					}
	      
				    val whereCond = s"annoquarti=${annoAggr.toInt} and mesequarti=${meseAggr.toInt} and annomesegiornodir <= ${ ghigliottina } ${ queryQuarti } ${ filesEsclusiQuery }"

				    
				    val query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" where ${whereCond}")
				    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
				    val tt = hiveContext.sql(query)


				    log.info(s"query spark.query.flusso_misure_quarti.time_stamp_max \n${query}")
						
						log.info("***** max_time_stamp OK")
						
						//tt.cache()
						
						
            val rdd = tt.map { r =>


							val pivaDistrib: String = "000000".concat(r.getString(0)) takeRight 11
							val pivautente: String = r.getString(1) //"000000".concat(r.getString(1))  takeRight 11
					  	val pod: String = r.getString(2)
							val anno: Int = r.getInt(3)
							val mese: Int = r.getInt(4) //s(4).toString
						  val giorno: Int = r.getInt(5)
							//    						val area:String =  r.getString(6).toString
							val validato: String = r.getString(7)
							val nomeFile: String = r.getString(8)
							val codcontrdisp: String = r.getString(9)
							val coduc: String = r.getString(10)
							val tipoE: Int = r.getInt(11)
							val tipoS: Int = r.getInt(12)
							val tensione: Double = r.getDouble(13)
							val trattO: String = r.getString(14)
							val potcontrimpl: Double = if (r.isNullAt(15)) 0 else r.getDouble(15)
							val potdisp: Double = if (r.isNullAt(16)) 0 else r.getDouble(16)
							val cifreatt: Int = if (r.isNullAt(17)) 0 else r.getInt(17)
							val cifrerea: Int = r.getInt(18)
							val raccolta: String = r.getString(19)
							val potmax: Double = r.getDouble(20)
							var perdita: Double = if (r.getDouble(21) == 219.999) perdita_220 else r.getDouble(21)

							val tensionetmp = tensione / 1000.0

							if (perdita == 0.0 && (tensionetmp != 0.0)) {
								if (tensionetmp.toInt < 1) {
									perdita = perdita_1 // 0.104 // 10.4%
								} else if (tensionetmp.toInt >= 1 && tensionetmp.toInt <= 35) {
									perdita = perdita_1_35 // 0.04 // 4%
								} else if (tensionetmp.toInt <= 150) {
									perdita = perdita_150 // 0.018 // 1.8%
								} else if (tensionetmp.toInt <= 220) {
									perdita = perdita_220 // 0.011 // 1.1
								} else if (tensionetmp.toInt > 220) {
									perdita = perdita_380 // 0.007
								} else {
									perdita = tensionetmp
								}
							}


							val annomesegiornodir: Int = r.getInt(22)
							val timestamp: Long = r.getLong(123)



							val area: String = if (estraiAreaApp && !testwithout_validations) {
								estrazioneArea(pod)
							} else {
								r.getString(6).toString
							}

							val valUdd = if (flaguddpodApp && !testwithout_validations) {
								validazioneOrariePodUdd(pod, pivautente, anno.toString, ("0".concat(mese.toString) takeRight 2), ("0".concat(giorno.toString) takeRight 2))
							} else {
                if(testwithout_validations)
                  ("N", "N", "SK")
                else
								  getNIDUdd(codcontrdisp) // ("SK","SK","SK") //
							}
							val valFlag = if ((statoApp || trattamentoApp) && !testwithout_validations) {
								validazioneStatoPod(pod)
							} else {
								("SK", "SK")
							}
							val valFlagArea = if (flagAreaApp && !testwithout_validations) {
								validazioneArea(pod, area)
							} else {
								"SK"
							}
							val distrAzienda = if (distrAziendaApp && !testwithout_validations) {
								getDistrAzienda(pivaDistrib)
							} else {
								("SK", "SK")
							}

							// S = scartato
							//UDD da query
						  val flaguddpod = valUdd._1 //valUdd.flaguddpod// scarta se N
						  val nIdUdd = valUdd._2 //valUdd.nIdUdd
					 	  val tPiva = valUdd._3 //valUdd.nIdUdd
						  val stato = valFlag._1 //valFlag.statopod
						  val trattamento = valFlag._2 //valFlag.isttrat //scarto se non è o
							//controllo tag validato S ok N ko            scarto se no

							//distributore-distributore di riferimento
							val nIdDistr: String = distrAzienda._1
							val nIdDistrRif: String = distrAzienda._2

							/*
      						 * se i singoli controlli sono attivati dal file di configurazione faccio i controlli altrimenti li considero validati
      						 */
							val flaguddpodVal = if (flaguddpodApp) {
								flaguddpod.equals("Y")
							} else {
								true
							}
							val trattamentoVal = if (trattamentoApp) {
								trattamento.toUpperCase.equals("Y")
							} else {
								true
							}
							val validatoVal = if (validatoApp) {
								validato.toUpperCase().equals("S")
							} else {
								true
							}
							val flagAreaVal = if (flagAreaApp) {
								valFlagArea.toUpperCase().equals("Y")
							} else {
								true
							}

							val flagValidazioni = if (flaguddpodVal && trattamentoVal && validatoVal && flagAreaVal) {
								"Y"
							} else {
								s"${if (!flaguddpodVal) "F" else ""}${if (!trattamentoVal) "T" else ""}${if (!validatoVal) "V" else ""}${if (!flagAreaVal) "A" else ""}"
							}

							val info = List(pivaDistrib, pivautente, pod, anno, mese, giorno, area, validato, nomeFile, codcontrdisp, coduc, tipoE, tipoS, tensione, trattO, potcontrimpl, potdisp, cifreatt, cifrerea, raccolta, potmax, perdita, annomesegiornodir)
							val hnValues = (23 to 122).map(r.getDouble(_)).grouped(4).map(_.sum * (1 + perdita)).toList
							val validazioni = List(timestamp, uidElab, dataelaborazione, flaguddpod, stato, trattamento, valFlagArea, nIdUdd, tPiva, nIdDistr, nIdDistrRif, flagValidazioni)

							Row.fromSeq(info ++ hnValues ++ validazioni)
						}
				     log.info("***** validazione OrariePodUdd, StatoPodArea, DistrAzienda OK")
						
				     
				     //rdd.cache()
				     log.info("***** rdd.cache OK")
				     
				     
    				
				     //log.info("***** xml scartati OK")
						
					  val dfQS1x = sqlCtx.createDataFrame(rdd, schemaOre)
            val dfQS1= dfQS1x.repartition(col("pod"))
						log.info("***** creazione DataFrame misure ore OK")
						
								
						dfQS1
						.write
						.format("parquet")
						.mode(SaveMode.Append)
						.partitionBy("anno","mese","pivadistributore","versione")
						.save(orarie)
						log.info("***** insert misure ore OK")
						
						
						/*
					   * aggiorno le partizioni
					   */
						if (true) {//(!commandLine.hasOption(commandLineOptions.local.getOpt)) {
							val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
									hiveContext.sql("MSCK REPAIR TABLE " + db + ".aggregazioni_misure_orarie" + (if(testwithout_validations)"_tmp" else ""))
									log.info("***** aggiornamento partizioni " + db + ".aggregazioni_misure_orarie" + (if(testwithout_validations)"_tmp" else "")+ " OK")
						}
						
						
				    /*
					   * chiudo le connessioni jdbc
					   */
				    try{  if(conn1 != null && !conn1.isClosed()){  conn1.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
				    try{  if(conn2 != null && !conn2.isClosed()){  conn2.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
				    try{  if(conn3 != null && !conn3.isClosed()){  conn3.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
				    try{  if(conn4 != null && !conn4.isClosed()){  conn4.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}
				    try{  if(conn5 != null && !conn5.isClosed()){  conn5.close() } }catch{case e: Exception => { log.error(e.getMessage, e)  }}

						sc.stop()

						log.info("***** Fine processo " + argsObj.appName + " *****")

			}

			/**
			 * Crea una vista utilizzata in fase di validazione.
			 * 
			 * @param annomese anno mese di riferimento.
			 */
			def creazioneVistaStatoPod(annomese:String) = {
					val queryJdbc = queryProp.getProperty("spark.query.createview").replaceAll("annomese", annomese)

					Class.forName(driver)
					val connection:Connection = DriverManager.getConnection(url, user, password)
					val ps = connection.createStatement()    
					log.info("*** executeUpdate " + ps.executeUpdate(queryJdbc))
					connection.close()
					log.info("***** creazione vista stato pod - trattamento OK")
			}
			

/**
 * Esegue una validazione Flag UDD-POD: assegna al flag Y N in  base alla completezza della relazione POD-UDD nel mese.
 * E' presente in un ciclo FOR che per ogni singolo giorno del mese nel file candidato per il POD in esame, 
 * controlla che esista la copertura in RCU_POD_UDD, la variabile giorno_valido è un tipo DATE.
 * 
 * @param pod identificativo del pod
 * @param tPiva partita iva UDD
 * @param anno 
 * @param mese
 * @param giorno di appartenenza del pod
 * @return flaguddpod flag di verifica
 */
			def validazioneOrariePodUdd(pod:String, tPiva:String, anno:String, mese:String,giorno:String) : (String,String,String) = {
					//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD')) 
					//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
					try{
						val ps1 = conn3.prepareStatement(queryPs1)
								ps1.setString(1, anno + mese + giorno)
//								ps1.setString(2, tPiva)
								ps1.setString(2, pod)
								val rs1 = ps1.executeQuery()

								val flaguddpod = if(rs1.next()){
								  val nIdUdd = rs1.getString("N_ID_UDD")
								  val tPiva = rs1.getString("T_PIVA")
									rs1.close()
									ps1.close()
									("Y", nIdUdd, tPiva)
								}else{
									rs1.close()
									ps1.close()
									("N", "N", "N")
								}
						//  								}

						flaguddpod
					}catch{
					case e: Exception => { e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						log.error(" pod:" + pod + " tPiva:" + tPiva + " data:" + anno + " " + mese + " "  + giorno)
						("E", "E", "E")
					}
					}

			}	
			
			/**
			 * Cerca la chiave primaria tramite il codice codcontrdisp se la trova altrimenti ritorna "N" utilizzato come codice d'errore.
			 * @param codcontrdisp 
 			 * @return chiave primaria
			 */
			def getNIDUdd(codcontrdisp:String) : (String,String,String) = {
					//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD')) 
					//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
					try{
						val ps1 = conn3.prepareStatement(queryPs11)
								ps1.setString(1, codcontrdisp)
								val rs1 = ps1.executeQuery()

								val nIdUdd = if(rs1.next()){
								  val ret = rs1.getString("N_ID_UDD")
									rs1.close()
									ps1.close()
									("Y", ret, "SK")
								}else{
									rs1.close()
									ps1.close()
//									ValUdd("N", "N")
									("N", "N", "SK")
								}
						//  								}

						nIdUdd
					}catch{
					case e: Exception => { e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						log.error(s" codcontrdisp: ${codcontrdisp}")
						("E","E","E")
					}
					}

			}	


			/**
			 * Valida lo stato del pod e il trattamento utilizzando la vista IS_T_TRATTAMENTO.
			 * @param pod identificativo del pod 
 			 * @return stato del pod e trattamento
			 */
			def validazioneStatoPod(pod:String) : (String,String) = {
					try{


						//spark.query.queryPs2=SELECT STATO_POD,IS_T_TRATTAMENTO FROM IS_T_TRATTAMENTO_STATO_POD  WHERE T_CODICE_POD= ?
						val ps2 = conn2.prepareStatement(queryPs2)
								ps2.setString(1, pod)
								val rs = ps2.executeQuery()
								if(rs.next() ){
									val statopod:String = rs.getString("STATO_POD")
											val isttrat:String = rs.getString("IS_T_TRATTAMENTO")
											rs.close()
											ps2.close()
											(statopod,isttrat)
								}else{
									rs.close()
									ps2.close()
									("N","N")
								}
					}catch{
					case e: Exception => {e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						("E","E")
					}
					}
			}
			
			/**
			 * Cerca la corrispondenza tra l'area acquisita dal file e quella registrata in RCU.
			 * @param pod identificativo del pod 
			 * @param area area acquisita dal file di misura
 			 * @return flag di validazione che può essere "Y" o "N"
			 */
			def validazioneArea(pod:String, area:String) : String = {
					try{

						//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
						val flagArea = {
								val ps2 = conn1.prepareStatement(queryPs4)
										ps2.setString(1, pod)
										ps2.setString(2, area)
										val rs = ps2.executeQuery()
										if(rs.next() ){
											rs.close()
											ps2.close()
											"Y"
										}else{
											rs.close()
											ps2.close()
											//								conn2.close()
											"N"
										}
						}
						
          flagArea
						
					}catch{
					case e: Exception => {e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						"E"
					}
					}
			}
			
     /**
			 * Ottiene l'area del pod registrata in RCU.
			 * @param pod identificativo del pod 
 			 * @return area
			 */
			def estrazioneArea(pod:String) : String = {
					try{

						//spark.query.queryPs4=SELECT 'Y' FROM RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14) AND T_AREA_RIF = ?
						val flagArea = {
								val ps5 = conn5.prepareStatement(queryPs3)
										ps5.setString(1, pod)
										val rs5 = ps5.executeQuery()
										if(rs5.next() ){
										  val ret = rs5.getString(1)
											rs5.close()
											ps5.close()
											ret
										}else{
											rs5.close()
											ps5.close()
											//								conn2.close()
											"N"
										}
						}
						
          flagArea
						
					}catch{
					case e: Exception => {e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						"E"
					}
					}
			}



			/**
			 * Cerca gli identificativi del distributore e del distributore di riferimento sulla base dati AU.
			 * 
			 * @param pivadistributore partita iva del distributore del file di misure
			 * @return DistrAzienda rappresenta gli identificativi N_ID_DISTR e N_ID_DISTR_RIF
			 */
			def getDistrAzienda(pivadistributore:String) : (String,String) = {
					try{
						//SELECT N_ID_DISTR,N_ID_DISTR_RIF FROM RCU.DISTR_AZ WHERE T_PIVA=?
						val ps2 = conn4.prepareStatement(queryDistrAzienda)
								ps2.setString(1, pivadistributore)
								val rs = ps2.executeQuery()
								val ret = if(rs.next() ){
								  val nIdDistr:String = rs.getString("N_ID_DISTR")
								  val nIdDistrRif:String = rs.getString("N_ID_DISTR_RIF")
								  rs.close()
									ps2.close()
									(nIdDistr,nIdDistrRif)
								}else{
								  rs.close()
									ps2.close()
									//Nomen Nescio
									("NN","NN")
								}
						ret
					}catch{
					case e: Exception => {e.printStackTrace()
						log.error("ERR: " + e.getMessage, e)
						("E","E")
					}
					}
			}
}