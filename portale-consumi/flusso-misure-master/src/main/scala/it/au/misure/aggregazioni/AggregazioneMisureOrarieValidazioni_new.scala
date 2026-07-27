package it.au.misure.aggregazioni

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.HashPartitioner
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
import scala.collection.mutable.HashMap
import it.au.misure.util.LoggingSupport
import org.apache.spark.storage.StorageLevel
import com.sun.org.apache.xalan.internal.xsltc.compiler.ValueOf

/**
	* ==Flusso Misure Aggregazione Misure Orarie==
	* Acquisisce le misure divise in quarti d'ora precedentemente elaborate dal processo
	* di ingestione denominato 'Flusso Misure Inserimento Misure Quarti' e le aggrega per ora. Al termine dell'aggregazione, il risultato viene salvato
	* nella tabella hdfs denominata ''aggregazioni_misure_orarie''.
	*/
object AggregazioneMisureOrarieValidazioni_new extends LoggingSupport {

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
			.setAppName( argsObj.appName +" New 2" )
			.set("spark.shuffle.service.enabled", "false")
			.set("spark.dynamicAllocation.enabled", "false")
			.set("spark.io.compression.codec", "snappy")
			.set("spark.rdd.compress", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

			.setMaster( argsObj.master )

		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		sc.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
		sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

		val minPartitions =  sc.getConf.get("spark.flusso.misure.min.partitions")

		val hiveCtx = new HiveContext(sc)

		hiveCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
		hiveCtx.setConf("spark.sql.parquet.compression.codec", "uncompressed")
		hiveCtx.setConf("spark.sql.parquet.binaryAsString", "true")
		hiveCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
		hiveCtx.setConf("hive.exec.dynamic.partition", "true")
		hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

		hiveCtx.setConf("spark.sql.parquet.mergeSchema", "false")
		hiveCtx.setConf("spark.sql.parquet.filterPushdown", "true")
		hiveCtx.setConf("spark.sql.hive.metastorePartitionPruning", "true")

		val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

		val annoAggr:String =  argsObj.anno
		val meseAggr:String =  argsObj.mese
		val ghigliottina:Int = argsObj.annomesegiornodir
		val quarti = sc.getConf.get("spark.flusso.misure.quarti")
		val orarie =  sc.getConf.get("spark.aggregazioni.misure.orarie")
		val tMillis = System.currentTimeMillis()
		val dataelaborazione = new java.sql.Timestamp( tMillis )
		val annomese = annoAggr + meseAggr
		val uidElab = sdf.format(new java.util.Date()).toLong
		log.info("***** Inizio processo " + argsObj.appName + " *****")
		log.info("***** SI CONSIGLIA DI ESEGUIRE IL BASH import_from_rcu_aggr.sh PRIMA DEL LANCIO")

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
    log.info("*** sc.master: " + sc.master)

		/*
     * creazione viste stato pod - trattamento
     */
		creazioneVistaStatoPod(annomese)


		val distr_oracle = sc.broadcast(this.getAllDistrAzienda())

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
    //val filesEsclusiQuery=""

    val whereCond = s"annoquarti=${annoAggr.toInt} and mesequarti=${meseAggr.toInt} and annomesegiornodir <= ${ ghigliottina } ${ queryQuarti } ${ filesEsclusiQuery }"


		var query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" where ${whereCond}")

		//val tt = hiveCtx.sql(query)

    log.info(s"query spark.query.flusso_misure_quarti.time_stamp_max \n${query}")

		//MEMORIZZO IN HASHTABLES LE VALIDAZIONI PREVISTE
	//	val tmp ="aggregato_periodico"+annomese
		//tt.registerTempTable(tmp)

		val annomese_aggr=annoAggr.toString+(("0" + meseAggr.toString) takeRight 2)

    val query_tmp =
      s"""
           LEFT OUTER JOIN (SELECT nomefile nf,pod,annomese from au.annullamento_pod_aggroraria where annomese=${annomese_aggr} and not(nomefile is null and pod is null)) mis_ann
           ON lcase(nomefile) = lcase(nvl(mis_ann.nf,nomefile)) and podquarti = nvl(mis_ann.pod,podquarti)
       """.stripMargin

    query = queryFlussoMisureQuartiTimeStampMax.replace("WHERE_CONDITIONS", s" ${query_tmp} where ${whereCond} and (mis_ann.annomese is null ) ")



    log.info("***** max_time_stamp OK")
		/*val queryannullamento=s"""
                          select aggr.* from (${query}) aggr
                           LEFT OUTER JOIN (SELECT  nomefile  , pod, annomese from au.annullamento_pod_aggroraria where annomese=${annomese_aggr} and nvl(nomefile,'')<>''  and nvl(pod,'') = '' ) mis_ann
                           ON ( lcase(aggr.nomefile) = lcase(mis_ann.nomefile) )
                           LEFT OUTER JOIN (SELECT  nomefile  , pod, annomese from au.annullamento_pod_aggroraria where annomese=${annomese_aggr} and nvl(nomefile,'')= ''  and nvl(pod,'') <> '' ) mis_ann2
                           ON ( aggr.podquarti = mis_ann2.pod )
                           LEFT OUTER JOIN (SELECT  nomefile  , pod, annomese from au.annullamento_pod_aggroraria where annomese=${annomese_aggr} and nvl(nomefile,'')<>''  and nvl(pod,'') <> '' ) mis_ann3
                           ON ( lcase(aggr.nomefile) = lcase(mis_ann3.nomefile) and aggr.podquarti = mis_ann3.pod )
                           WHERE (mis_ann.annomese is not null or mis_ann2.annomese is not null or mis_ann3.annomese is not null)
			"""*/


    log.info(s"Estrazioni misure da escludere per il periodo ${annoAggr} dalla tabella au.annullamento_pod_aggroraria")
		val dtt_tmp=hiveCtx.sql(query)//.persist(StorageLevels.MEMORY_ONLY_SER)
		val tmp2 ="aggregato_periodico_a"+annomese
		dtt_tmp.registerTempTable(tmp2)

		/*checkTrattamento(annomese_aggr,hiveCtx,tmp)

		val q_data_validare1=s"""SELECT DISTINCT podquarti,areaquarti,nvl(is_orario,'')is_orario FROM ${tmp} quarti
													 	 LEFT OUTER JOIN (SELECT codice_pod t_pod,is_orario FROM misure.pods_orari where annomese =${annomese_aggr}) pods
														 ON pods.t_pod = quarti.podquarti
			                    """
    */

		//INDIVIDUAZIONE POD ORARI
		val objEstrMisEE=it.au.misure.portale.consumi.Estrazione_Misure_EE_new
		log.info(s"Individuazione pod con trattamento orario per annomese ${annomese_aggr}")
		objEstrMisEE.TrovaValidazioneStatoPod(hiveCtx,annomese_aggr)


		/*val queryValidazioni=s"""SELECT AG.*,nvl(v_pods.STATO_POD,'')STATO_POD,nvl(v_pods.IS_T_TRATTAMENTO,'')IS_T_TRATTAMENTO,
                         nvl(p_area.T_AREA_RIF,'')area_rif  FROM ${tmp2} AG
                         LEFT OUTER JOIN validazione_pod v_pods ON v_pods.T_CODICE_POD = substr(podquarti,1,14)
                         LEFT OUTER JOIN RCU.RCU_POD_p p_area ON  substr(p_area.T_CODICE_POD,1,14)= substr(podquarti,1,14)
                         """*/

		val queryValidazioni=s"""SELECT AG.*,nvl(v_pods.STATO_POD,'')STATO_POD,nvl(v_pods.IS_T_TRATTAMENTO,'')IS_T_TRATTAMENTO
                         FROM ${tmp2} AG
                         LEFT OUTER JOIN validazione_pod v_pods ON v_pods.T_CODICE_POD = substr(podquarti,1,14)
                         """

		val dtt = hiveCtx.sql(queryValidazioni)
		dtt.cache()

		/*val q_data_validare1=s"""SELECT DISTINCT podquarti,areaquarti,nvl(v_pods.STATO_POD,'')STATO_POD,nvl(v_pods.IS_T_TRATTAMENTO,'')IS_T_TRATTAMENTO
                             FROM ${tmp2} quarti
														 LEFT OUTER JOIN validazione_pod v_pods ON T_CODICE_POD = substr(quarti.podquarti,1,14)
                           """

		log.info("Avvio validazioni sui campi podquarti,codcontrdispquarti,areaquarti")

		val dt_tmp_pods_area = hiveCtx.sql(q_data_validare1)

		val rdd_pods_area =dt_tmp_pods_area.map{ erow =>

			val pod: String = erow.getString(0)
			val areaquarti: String = erow.getString(1)
			val statopod : String = if(erow.getString(2).trim()=="1")"Y" else if(erow.getString(2).trim()=="0") "N" else ""
			val is_orario : String =  if(erow.getString(3).trim()=="1")"Y" else if(erow.getString(3).trim()=="0") "N" else ""

			var area: String = ""
			var dictArea:String = ""


			area = if (estraiAreaApp) {
				val tmp = estrazioneArea(pod)
				dictArea = pod +"#"+tmp
				tmp
			} else {
				dictArea="#N"
				areaquarti
			}

			val dictArea2 = if (flagAreaApp) {
				val tmp = validazioneArea(pod, area)
				pod + area+"#"+tmp
			}else "#N"

			val dictStatoPods =if ((statoApp || trattamentoApp) && is_orario=="") {
				val tmp = validazioneStatoPod(pod)
				pod+"#"+tmp._1+"_"+tmp._2

			}else {
				pod+s"#${statopod}_${is_orario}"
			}


			/*val dictUdd =if( flaguddpodApp)
				{
					val tmp =validazioneOrariePodUddMese(pod,annomese)
					pod+"#"+tmp._1+"_"+tmp._2+"_"+tmp._3
				}else "#N_N_N"*/

			val dictUdd ="#D_N_N"


			//(dictArea,dictArea2,dictStatoPods,dictUdd)
			(dictArea,dictArea2,dictStatoPods,dictUdd)
		}.collect()


		//hiveCtx.dropTempTable(tmp)

		log.info("Memorizzazione validazioni in hashtables")

		var dictArea:Map[String,String] = Map("" -> "")
		var dictArea2:Map[String,String] = Map("" -> "")
		var dictUdd:Map[String,(String,String,String)]  = Map("" -> ("","",""))
		var dictStatoPods:Map[String,(String,String)] = Map("" -> ("",""))


		log.info("num recs tot : " + (rdd_pods_area.length).toString )

		//var cc=0
		for (erow <- rdd_pods_area) {


			val area1= erow._1.split("#")
			val area2= erow._2.split("#")
			val statopods= erow._3.split("#")
			val udd = erow._4.split("#")


			if(estraiAreaApp && area1(0) !="" && !dictArea.contains(area1(0)))
			dictArea+=(area1(0) -> area1(1))

			if(flagAreaApp && area2(0) !="" && !dictArea2.contains(area2(0)))
				dictArea2+=(area2(0) -> area2(1))

			if((statoApp || trattamentoApp) && !dictStatoPods.contains(statopods(0)) && statopods(0) !="" ) {
				val vals= statopods(1).split("_")
				dictStatoPods += (statopods(0) -> (vals(0),vals(1)))
			}

			if( flaguddpodApp && !dictUdd.contains(udd(0)) && udd(0) !="")
				{
					val vals= udd(1).split("_")
					dictUdd += (udd(0) -> (vals(0),vals(1),vals(2)))
				}
		}




		if(!flaguddpodApp) {

			val rdd_codcontrdisp = {
				val q_data_validare2=s"SELECT DISTINCT codcontrdispquarti FROM ${tmp2} "
				val dt_tmp_codcontrdisp = hiveCtx.sql(q_data_validare2)

				dt_tmp_codcontrdisp.map { erow =>

					val codcontrdisp: String = erow.getString(0)
					var dictUdd: String = ""


					val tmp = getNIDUdd(codcontrdisp) // ("SK","SK","SK") //
					dictUdd = codcontrdisp + "#" + tmp._1 + "_" + tmp._2 + "_" + tmp._3

					if(dictUdd=="")dictUdd="#N_N_N"

					dictUdd
				}.collect()
			}

			for (erow <- rdd_codcontrdisp) {


				val udd = erow.split("#")

				if (!flaguddpodApp && !dictUdd.contains(udd(0)) && udd(0) !="" ) {
					val vals = udd(1).split("_")
					dictUdd += (udd(0) -> (vals(0), vals(1), vals(2)))
				}
			}
		}

		val dictonArea1=sc.broadcast(dictArea)
		val dictonArea2=sc.broadcast(dictArea2)
		val dictonUdd= sc.broadcast(dictUdd)
		val dictonStatoPods=sc.broadcast(dictStatoPods)

		//FINE ISTRUZIONI DI TEST

    hiveCtx.dropTempTable(tmp2)
		log.info("Memorizzazione validazioni in hashtables OK")
  */

		val rdd = dtt.map { r =>


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


			val statoPod = r.getAs[String]("STATO_POD").trim()
			val statoPod_x = if (statoPod == "1") "Y" else if (statoPod == "0") "N" else ""
			val istrattPod = r.getAs[String]("IS_T_TRATTAMENTO").trim()
			val istrattPod_x = if (istrattPod == "1") "Y" else if (istrattPod == "0") "N" else ""
			val arearif = "" //r.getAs[String]("area_rif").trim()

			val area: String = if (estraiAreaApp) {
				/*if(dictonArea1.value.contains(pod))  dictonArea1.value(pod)
				else*/
				log.info(s"estrazione area : ${arearif}")
				if (arearif == "") estrazioneArea(pod)
				else arearif
			} else {
				r.getString(6).toString
			}
			val inizio = System.currentTimeMillis() / 1000

			val valUdd = if (flaguddpodApp) {
				log.info("validazione orarie pod-udd")
				validazioneOrariePodUdd(pod, anno.toString, ("0".concat(mese.toString) takeRight 2), ("0".concat(giorno.toString) takeRight 2))
			} else {
				log.info("validazione orarie getNIDUdd")
				getNIDUdd(codcontrdisp) // ("SK","SK","SK") //
			}
			val valFlag = if (statoApp || trattamentoApp) {
				if (statoPod_x == "" || istrattPod_x == "") {

					log.info("trattamento pod oracle")
					validazioneStatoPod(pod)
				}
				else {
					log.info("trattamento pod cloudera")
					(statoPod_x, istrattPod_x)
				}
			} else {
				("SK", "SK")
			}
			val valFlagArea = if (flagAreaApp) {
				log.info("validazione area")
				validazioneArea(pod, area)
			} else {
				"SK"
			}
			val distrAzienda = if (distrAziendaApp) {
				log.info("get distributore")
				getDistrAzienda(pivaDistrib, distr_oracle)
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

			//val kk_row=anno.toString+mese.toString+pivaDistrib+codcontrdisp

			//(kk_row,Row.fromSeq( info ++ hnValues ++ validazioni ))

			val fine = System.currentTimeMillis() / 1000
			log.info(s"***** tempo esecuzione ${((fine - inizio) / 60)}:${((fine - inizio) % 60)}")

			Row.fromSeq(info ++ hnValues ++ validazioni)
		}


		log.info("***** validazione OrariePodUdd, StatoPodArea, DistrAzienda OK")




		//log.info("***** xml scartati OK")

		val dfQS1 = hiveCtx.createDataFrame(rdd, schemaOre)
		log.info("***** creazione DataFrame misure ore OK")


		log.info("Scrittura in " + orarie)
		dfQS1
			.write
			.format("parquet")
			.mode(SaveMode.Append)
			.partitionBy("anno","mese","pivadistributore","versione")
			.save(orarie)

		log.info("***** insert misure ore OK su ")

		//dtt.unpersist()


		/*
     * aggiorno le partizioni
     */
		  hiveCtx.sql("DROP TABLE IF EXISTS validazione_pod")

		if(propertiesC.PortaleConsumi34Mesi)
			hiveCtx.sql("MSCK REPAIR TABLE au_test.aggregazioni_misure_orarie")
		 else
			hiveCtx.sql("MSCK REPAIR TABLE " + db + ".aggregazioni_misure_orarie")



		log.info("***** aggiornamento partizioni OK")
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


	def checkTrattamento(annomese_aggr:String,hiveCtx:HiveContext,tblquarti:String): Unit ={


		val dt_tmp_pods = hiveCtx.sql(s"""SELECT DISTINCT podquarti,'${annomese_aggr}' annomese_check
                                  FROM ${tblquarti} quarti
                                  LEFT OUTER JOIN (SELECT codice_pod t_pod FROM misure.pods_orari where annomese =${annomese_aggr}) pods
                                  ON pods.t_pod = quarti.podquarti
			                            where  pods.t_pod is null""")

		val rdd_pods_Ora =dt_tmp_pods.map { erow =>
			val pod: String = erow.getString(0)
			val an_ms :String = erow.getString(1)
			//val stato :String = erow.getString(2)
			//val trattamento :String = erow.getString(3)

			val isOrario = validazioneStatoPod(pod)._2.toUpperCase.equals("Y")

			if(isOrario)
				Row.fromSeq(List(pod,"1",an_ms.toInt))
			else
				Row.fromSeq(List(pod,"0",an_ms.toInt))
		}


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


	def validazioneOrariePodUddMese(pod:String, annomesestr:String) : (String,String,String) = {

		try{
			val dt = (annomesestr+"01").toInt
			val queryUdd =
			s"""
		    SELECT  N_ID_UDD, T_PIVA ,TO_CHAR(NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')),'YYYYMMDD') D_FINE
				 FROM POD_POD_UDD_STOR_CM
				 WHERE POD_14= substr('${pod}',1,14)
  		""".stripMargin

			val ps1 = conn3.prepareStatement(queryUdd)
			val rs1 = ps1.executeQuery()
			var cc=0
			var rt_val:(String,String,String)=("N","N","N")
			while (rs1.next())
				{
					val dfine=rs1.getInt("D_FINE")
					if(dfine>=dt) {
						cc=cc+1
						rt_val=("Y", rs1.getString("N_ID_UDD"),  rs1.getString("T_PIVA"))
					}
				}
			rt_val = if(cc==0) ("N","N","N")
							 else if(cc>1)("D","N","N")
			         else rt_val


			rs1.close()
			ps1.close()

			rt_val
		}catch{
			case e: Exception => { e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				log.error(" pod:" + pod + " annomese:" + annomesestr  )
				("E", "E", "E")
			}
		}

	}

	/**
		* Esegue una validazione Flag UDD-POD: assegna al flag Y N in  base alla completezza della relazione POD-UDD nel mese.
		* E' presente in un ciclo FOR che per ogni singolo giorno del mese nel file candidato per il POD in esame,
		* controlla che esista la copertura in RCU_POD_UDD, la variabile giorno_valido è un tipo DATE.
		*
		* @param pod identificativo del pod
		* @param anno
		* @param mese
		* @param giorno di appartenenza del pod
		* @return flaguddpod flag di verifica
		*/
	def validazioneOrariePodUdd(pod:String, anno:String, mese:String,giorno:String) : (String,String,String) = {
		//spark.query.queryPs1=SELECT 'Y'  FROM POD_POD_UDD WHERE TO_DATE(?,'YYYYMMDD') between NVL(D_INIZIO,TO_DATE('19000101','YYYYMMDD'))
		//AND NVL(D_FINE,TO_DATE('20991231','YYYYMMDD')) and T_PIVA = ? and T_CODICE_POD=?
		try{
			val ps1 = conn3.prepareStatement(queryPs1)
			ps1.setString(1, anno + mese + giorno)

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
				log.error(" pod:" + pod + " data:" + anno + " " + mese + " "  + giorno)
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

			//spark.query.queryPs3=SELECT T_AREA_RIF FROM RCU.RCU_POD WHERE substr(T_CODICE_POD,1,14) = substr(?,1,14)
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


	def getAllDistrAzienda() : Map[String,String] = {
		var dictionary = Map(""->"")
		try{

			val conn:Connection = DriverManager.getConnection(url, user, password)

			//estraggo le definizioni di tutti i distributori
			val ps = conn.prepareStatement("SELECT T_PIVA,N_ID_DISTR, N_ID_DISTR_RIF FROM DISTR_AZ")
			val rs = ps.executeQuery()
			var cc=0
			while(rs.next() ){
				val pivadistr:String = rs.getString("T_PIVA")
				val nIdDistr:String = rs.getString("N_ID_DISTR")
				val nIdDistrRif:String = rs.getString("N_ID_DISTR_RIF")
				dictionary += (pivadistr -> (nIdDistr+"_"+nIdDistrRif))
				cc=cc+1
			}
			rs.close()
			ps.close()

			log.info("Tot distributori letti : " + cc.toString)

			dictionary
		}catch{
			case e: Exception => {e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				dictionary
			}
		}
	}


	/**
		* Cerca gli identificativi del distributore e del distributore di riferimento sulla base dati AU.
		*
		* @param pivadistributore partita iva del distributore del file di misure
		* @return DistrAzienda rappresenta gli identificativi N_ID_DISTR e N_ID_DISTR_RIF
		*/
	def getDistrAzienda(pivadistributore:String,distr_oracle:Broadcast[Map[String,String]]) : (String,String) = {

		try
		{
			if(distr_oracle.value.contains(pivadistributore)){
				val data= distr_oracle.value(pivadistributore).split("_")
				val nIdDistr:String = data(0)
				val nIdDistrRif:String = data(1)
				(nIdDistr,nIdDistrRif)
			}
			else
				("NN","NN")
		}
		catch {
			case e: Exception => {
				e.printStackTrace()
				log.error("ERR: " + e.getMessage, e)
				("E", "E")
			}
		}
	/*def getDistrAzienda(pivadistributore:String) : (String,String) = {
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
		}*/
	}
}